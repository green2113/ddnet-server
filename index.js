import 'dotenv/config'
import express from 'express'
import cors from 'cors'
import cookieParser from 'cookie-parser'
import session from 'express-session'
import passport from 'passport'
import { Strategy as DiscordStrategy } from 'passport-discord'
import { createServer } from 'http'
import { Server as SocketIOServer } from 'socket.io'
import { randomUUID } from 'crypto'
import axios from 'axios'
import fs from 'fs'
import path from 'path'

const app = express()
const ORIGIN = process.env.WEB_ORIGIN || ''
if (!ORIGIN) {
  console.warn('[WARN] WEB_ORIGIN is not set. Falling back to http://localhost:5173 for redirects.')
}
const httpServer = createServer(app)
const io = new SocketIOServer(httpServer, {
  cors: {
    origin: [process.env.WEB_ORIGIN || 'http://localhost:5173'],
    credentials: true,
  },
})

app.use(cors({ origin: ORIGIN || 'http://localhost:5173', credentials: true }))
app.use(express.json())
app.use(cookieParser())
// trust proxy for correct secure cookies behind Fly/Proxies
app.set('trust proxy', 1)

// Message history in memory + JSONL file persistence
const MESSAGE_HISTORY_LIMIT = Number(process.env.MESSAGE_HISTORY_LIMIT || 500)
const messageHistory = []

// JSONL persistence settings
const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data')
const MESSAGES_FILE = process.env.MESSAGES_FILE || path.join(DATA_DIR, 'messages.jsonl')

function ensureDataDir() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true })
  } catch (err) {
    console.error('[messages] failed to create data dir:', err?.message || err)
  }
}

function appendMessageToFile(message) {
  try {
    const line = JSON.stringify(message) + '\n'
    fs.promises.appendFile(MESSAGES_FILE, line).catch((err) => {
      console.error('[messages] append failed:', err?.message || err)
    })
  } catch (err) {
    console.error('[messages] append error:', err?.message || err)
  }
}

function loadHistoryFromFile() {
  ensureDataDir()
  if (!fs.existsSync(MESSAGES_FILE)) return
  try {
    const content = fs.readFileSync(MESSAGES_FILE, 'utf8')
    const lines = content.split(/\r?\n/)
    for (const line of lines) {
      if (!line.trim()) continue
      try {
        const parsed = JSON.parse(line)
        if (parsed && typeof parsed === 'object') {
          messageHistory.push(parsed)
        }
      } catch {
        // ignore bad line
      }
    }
    // keep only the latest up to limit
    if (messageHistory.length > MESSAGE_HISTORY_LIMIT) {
      const start = messageHistory.length - MESSAGE_HISTORY_LIMIT
      messageHistory.splice(0, start)
    }
    console.log(`[messages] loaded ${messageHistory.length} messages from file`)
  } catch (err) {
    console.error('[messages] load failed:', err?.message || err)
  }
}

loadHistoryFromFile()

const isHttps = (ORIGIN || '').startsWith('https://')
const sessionMiddleware = session({
  secret: process.env.SESSION_SECRET || 'dev_secret_change_me',
  resave: false,
  saveUninitialized: false,
  cookie: { secure: isHttps, sameSite: 'none' },
})
app.use(sessionMiddleware)

passport.serializeUser((user, done) => {
  done(null, user)
})
passport.deserializeUser((obj, done) => {
  done(null, obj)
})

passport.use(
  new DiscordStrategy(
    {
      clientID: process.env.DISCORD_CLIENT_ID,
      clientSecret: process.env.DISCORD_CLIENT_SECRET,
      callbackURL: process.env.DISCORD_CALLBACK_URL || 'http://localhost:4000/auth/discord/callback',
      scope: ['identify'],
    },
    (accessToken, refreshToken, profile, done) => {
      const displayName = profile.global_name || profile.displayName || profile.username
      const avatarUrl = profile.avatar
        ? `https://cdn.discordapp.com/avatars/${profile.id}/${profile.avatar}.png?size=128`
        : `https://cdn.discordapp.com/embed/avatars/0.png`
      const user = {
        id: profile.id,
        username: profile.username,
        displayName,
        avatar: avatarUrl,
      }
      return done(null, user)
    },
  ),
)

app.use(passport.initialize())
app.use(passport.session())

app.get('/auth/discord', passport.authenticate('discord'))
app.get(
  '/auth/discord/callback',
  passport.authenticate('discord', {
    failureRedirect: `${ORIGIN || 'http://localhost:5173'}/`,
  }),
  (req, res) => {
    // 로그인 성공: 프런트엔드로 복귀
    res.redirect(ORIGIN || 'http://localhost:5173')
  },
)

app.post('/auth/logout', (req, res) => {
  req.logout(() => {
    req.session.destroy(() => {
      res.clearCookie('connect.sid')
      res.sendStatus(204)
    })
  })
})

app.get('/api/me', (req, res) => {
  if (req.user) return res.json(req.user)
  res.json(null)
})

app.get('/api/history', (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 200, MESSAGE_HISTORY_LIMIT)
  const start = Math.max(messageHistory.length - limit, 0)
  res.json(messageHistory.slice(start))
})

// (Discord 봇 미사용) 디스코드 채널 브릿지는 제거되었습니다.

io.engine.use(sessionMiddleware)
io.use((socket, next) => {
  next()
})

io.on('connection', (socket) => {
  socket.on('chat:send', async (payload) => {
    const sess = socket.request?.session
    const sessUser = sess?.passport?.user
    if (!sessUser) {
      return // ignore unauthenticated send
    }
    const message = {
      id: randomUUID(),
      author: {
        id: sessUser?.id || 'web',
        username: sessUser?.username || 'WebUser',
        displayName: sessUser?.displayName || sessUser?.username || 'WebUser',
        avatar: sessUser?.avatar || null,
      },
      content: String(payload?.content || ''),
      timestamp: Date.now(),
      source: payload?.source === 'ddnet' ? 'ddnet' : 'web',
    }

    // Broadcast to web clients
    io.emit('chat:message', message)

    // Save into in-memory history
    messageHistory.push(message)
    if (messageHistory.length > MESSAGE_HISTORY_LIMIT) messageHistory.shift()

    // Persist to JSONL file
    appendMessageToFile(message)

    // Bridge to DDNet webhook if provided
    if (process.env.DDNET_WEBHOOK_URL && message.content) {
      try {
        await axios.post(
          process.env.DDNET_WEBHOOK_URL,
          {
            content: message.content,
            author: message.author.username,
            source: 'web',
          },
          { timeout: 5000 },
        )
      } catch (err) {
        console.error('Failed to bridge to DDNet webhook:', err?.message || err)
      }
    }
  })
})

const port = Number(process.env.PORT || 4000)
httpServer.listen(port, () => {
  console.log(`Server listening on http://localhost:${port}`)
})

// HTTP endpoint to accept DDNet -> Web incoming messages
app.post('/bridge/ddnet/incoming', (req, res) => {
  const { content, author, timestamp } = req.body || {}
  if (!content || typeof content !== 'string') return res.status(400).json({ error: 'content required' })
  const bridged = {
    id: randomUUID(),
    author: { id: 'ddnet', username: author || 'DDNet' },
    content,
    timestamp: timestamp || Date.now(),
    source: 'ddnet',
  }
  io.emit('chat:message', bridged)
  messageHistory.push(bridged)
  if (messageHistory.length > MESSAGE_HISTORY_LIMIT) messageHistory.shift()
  appendMessageToFile(bridged)
  res.sendStatus(204)
})


