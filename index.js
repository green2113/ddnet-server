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
import { MongoClient } from 'mongodb'

const app = express()
const ADMIN_ID = '776421522188664843'
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

// Message history in memory (fallback when MongoDB is not configured)
const MESSAGE_HISTORY_LIMIT = Number(process.env.MESSAGE_HISTORY_LIMIT || 500)
const messageHistory = []

const defaultChannels = () => [
  { id: 'general', name: 'general', type: 'text', hidden: false, createdAt: Date.now(), createdBy: 'system' },
  { id: 'ddnet-bridge', name: 'ddnet-bridge', type: 'text', hidden: false, createdAt: Date.now(), createdBy: 'system' },
]

let channels = defaultChannels()

const isAdminId = (userId) => userId === ADMIN_ID

async function listChannels() {
  if (channelsCol) {
    const rows = await channelsCol.find({}, { projection: { _id: 0 } }).toArray()
    return rows.map((row) => ({ ...row, type: row.type === 'voice' ? 'voice' : 'text', hidden: !!row.hidden }))
  }
  return channels.map((channel) => ({ ...channel, type: channel.type === 'voice' ? 'voice' : 'text' }))
}

async function getChannelById(channelId) {
  if (channelsCol) {
    return channelsCol.findOne({ id: channelId }, { projection: { _id: 0 } })
  }
  return channels.find((channel) => channel.id === channelId) || null
}

async function upsertChannel(channel) {
  if (channelsCol) {
    await channelsCol.updateOne({ id: channel.id }, { $set: channel }, { upsert: true })
    return
  }
  const idx = channels.findIndex((row) => row.id === channel.id)
  if (idx >= 0) {
    channels[idx] = channel
  } else {
    channels.push(channel)
  }
}

async function removeChannel(channelId) {
  if (channelsCol) {
    await channelsCol.deleteOne({ id: channelId })
    return
  }
  channels = channels.filter((channel) => channel.id !== channelId)
}

// Prefer MongoDB if configured; otherwise use JSONL file
let mongoClient
let messagesCol
let channelsCol
async function initMongo() {
  const uri = process.env.MONGODB_URI
  if (!uri) return
  mongoClient = new MongoClient(uri)
  await mongoClient.connect()
  const db = mongoClient.db(process.env.MONGO_DB || 'ddnet')
  messagesCol = db.collection(process.env.MONGO_COLL || 'messages')
  await messagesCol.createIndex({ ts: 1 })
  channelsCol = db.collection(process.env.MONGO_CHANNELS_COLL || 'channels')
  await channelsCol.createIndex({ name: 1 }, { unique: true })
  const existing = await channelsCol.countDocuments()
  if (existing === 0) {
    await channelsCol.insertMany(defaultChannels())
  }
  console.log('[mongo] connected')
}
initMongo().catch((e) => console.error('[mongo] init failed', e?.message || e))

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

app.get('/api/channels', async (req, res) => {
  try {
    const userId = req.user?.id
    const allChannels = await listChannels()
    const visible = isAdminId(userId) ? allChannels : allChannels.filter((channel) => !channel.hidden)
    res.json(visible)
  } catch (e) {
    console.error('[channels] list failed', e?.message || e)
    res.status(500).json({ error: 'failed to load channels' })
  }
})

app.post('/api/channels', async (req, res) => {
  const userId = req.user?.id
  if (!isAdminId(userId)) return res.status(403).json({ error: 'forbidden' })
  const name = String(req.body?.name || '').trim()
  if (!name) return res.status(400).json({ error: 'name required' })
  const type = req.body?.type === 'voice' ? 'voice' : 'text'
  const channel = {
    id: randomUUID(),
    name,
    type,
    hidden: false,
    createdAt: Date.now(),
    createdBy: userId,
  }
  try {
    await upsertChannel(channel)
    io.emit('channels:update')
    res.status(201).json(channel)
  } catch (e) {
    console.error('[channels] create failed', e?.message || e)
    res.status(500).json({ error: 'failed to create channel' })
  }
})

app.delete('/api/channels/:id', async (req, res) => {
  const userId = req.user?.id
  if (!isAdminId(userId)) return res.status(403).json({ error: 'forbidden' })
  const channelId = req.params.id
  try {
    await removeChannel(channelId)
    if (messagesCol) {
      await messagesCol.deleteMany({ channelId })
    } else {
      let idx = messageHistory.length
      while (idx--) {
        if (messageHistory[idx].channelId === channelId) messageHistory.splice(idx, 1)
      }
    }
    io.emit('channels:update')
    res.sendStatus(204)
  } catch (e) {
    console.error('[channels] delete failed', e?.message || e)
    res.status(500).json({ error: 'failed to delete channel' })
  }
})

app.patch('/api/channels/:id/hidden', async (req, res) => {
  const userId = req.user?.id
  if (!isAdminId(userId)) return res.status(403).json({ error: 'forbidden' })
  const channelId = req.params.id
  const hidden = Boolean(req.body?.hidden)
  try {
    const channel = await getChannelById(channelId)
    if (!channel) return res.status(404).json({ error: 'channel not found' })
    const updated = { ...channel, hidden }
    await upsertChannel(updated)
    io.emit('channels:update')
    res.json(updated)
  } catch (e) {
    console.error('[channels] hide failed', e?.message || e)
    res.status(500).json({ error: 'failed to update channel' })
  }
})

// Normalize stored rows (both legacy flat docs and new nested docs) to message shape
function normalizeMessageRow(row) {
  const hasNestedAuthor = row && typeof row === 'object' && row.author && typeof row.author === 'object'
  const author = hasNestedAuthor
    ? {
        id: row.author.id || row.user_id || 'web',
        username: row.author.username || row.username || 'WebUser',
        displayName: row.author.displayName || row.display_name || row.username || 'WebUser',
        avatar: row.author.avatar ?? row.avatar ?? null,
      }
    : {
        id: row?.user_id || 'web',
        username: row?.username || 'WebUser',
        displayName: row?.display_name || row?.username || 'WebUser',
        avatar: row?.avatar ?? null,
      }
  return {
    id: row?.id,
    author,
    content: row?.content || '',
    source: row?.source || 'web',
    channelId: row?.channelId || 'general',
    timestamp: row?.timestamp || row?.ts || Date.now(),
  }
}

app.get('/api/history', async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 200, MESSAGE_HISTORY_LIMIT)
  const channelId = String(req.query.channelId || 'general')
  if (messagesCol) {
    try {
      const rows = await messagesCol
        .find({ channelId }, { projection: { _id: 0 } })
        .sort({ ts: 1 })
        .limit(limit)
        .toArray()
      const normalized = rows.map(normalizeMessageRow)
      return res.json(normalized)
    } catch (e) {
      console.error('[mongo] history failed', e?.message || e)
    }
  }
  const filtered = messageHistory.filter((message) => message.channelId === channelId)
  const start = Math.max(filtered.length - limit, 0)
  res.json(filtered.slice(start))
})

// (Discord 봇 미사용) 디스코드 채널 브릿지는 제거되었습니다.

io.engine.use(sessionMiddleware)
io.use((socket, next) => {
  next()
})

const voiceMembers = new Map()

const emitVoiceMembers = (channelId) => {
  const members = Array.from(voiceMembers.get(channelId)?.values() || [])
  io.to(`voice:${channelId}`).emit('voice:members', { channelId, members })
}

io.on('connection', (socket) => {
  socket.on('chat:send', async (payload) => {
    const sess = socket.request?.session
    const sessUser = sess?.passport?.user
    if (!sessUser) {
      return // ignore unauthenticated send
    }
    const channelId = typeof payload?.channelId === 'string' ? payload.channelId : 'general'
    const channel = await getChannelById(channelId)
    if (!channel) return
    if (channel.hidden && !isAdminId(sessUser?.id)) return
    const message = {
      id: randomUUID(),
      author: {
        id: sessUser?.id || 'web',
        username: sessUser?.username || 'WebUser',
        displayName: sessUser?.displayName || sessUser?.username || 'WebUser',
        avatar: sessUser?.avatar || null,
      },
      content: String(payload?.content || ''),
      channelId,
      timestamp: Date.now(),
      source: payload?.source === 'ddnet' ? 'ddnet' : 'web',
    }

    // Broadcast to web clients
    io.emit('chat:message', message)

    // Persist
    if (messagesCol) {
      // Store in the same shape as runtime message for simplicity; keep ts for sorting/index
      const doc = { ...message, ts: message.timestamp }
      messagesCol.insertOne(doc).catch((e) => console.error('[mongo] insert failed', e?.message || e))
    } else {
      messageHistory.push(message)
      if (messageHistory.length > MESSAGE_HISTORY_LIMIT) messageHistory.shift()
    }

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

  // Delete message (owner only)
  socket.on('chat:delete', async (payload) => {
    try {
      const messageId = payload?.id
      if (!messageId) return
      const sessUser = socket.request?.session?.passport?.user
      if (!sessUser) return

      let deleted = false
      if (messagesCol) {
        // Find the message to verify ownership
        const found = await messagesCol.findOne({ id: messageId }, { projection: { author: 1, user_id: 1 } })
        const authorId = found?.author?.id || found?.user_id
        if (authorId && authorId === sessUser.id) {
          const r = await messagesCol.deleteOne({ id: messageId })
          deleted = r.deletedCount > 0
        }
      } else {
        const idx = messageHistory.findIndex((m) => m.id === messageId)
        if (idx >= 0 && messageHistory[idx].author?.id === sessUser.id) {
          messageHistory.splice(idx, 1)
          deleted = true
        }
      }

      if (deleted) {
        io.emit('chat:delete', messageId)
      }
    } catch (err) {
      console.error('[delete] failed', err?.message || err)
    }
  })

  socket.on('voice:join', async (payload) => {
    const sessUser = socket.request?.session?.passport?.user
    if (!sessUser) return
    const channelId = String(payload?.channelId || '')
    if (!channelId) return
    const channel = await getChannelById(channelId)
    if (!channel || channel.type !== 'voice') return
    if (!voiceMembers.has(channelId)) {
      voiceMembers.set(channelId, new Map())
    }
    const channelMembers = voiceMembers.get(channelId)
    channelMembers.set(socket.id, {
      id: socket.id,
      username: sessUser.username,
      displayName: sessUser.displayName,
      avatar: sessUser.avatar || null,
    })
    socket.join(`voice:${channelId}`)
    emitVoiceMembers(channelId)
  })

  socket.on('voice:leave', (payload) => {
    const channelId = String(payload?.channelId || '')
    if (!channelId) return
    const channelMembers = voiceMembers.get(channelId)
    if (channelMembers?.has(socket.id)) {
      channelMembers.delete(socket.id)
      socket.leave(`voice:${channelId}`)
      emitVoiceMembers(channelId)
      io.to(`voice:${channelId}`).emit('voice:leave', { channelId, peerId: socket.id })
    }
  })

  socket.on('voice:offer', (payload) => {
    const channelId = String(payload?.channelId || '')
    const targetId = String(payload?.targetId || '')
    if (!channelId || !targetId) return
    io.to(targetId).emit('voice:offer', { channelId, fromId: socket.id, sdp: payload?.sdp })
  })

  socket.on('voice:answer', (payload) => {
    const channelId = String(payload?.channelId || '')
    const targetId = String(payload?.targetId || '')
    if (!channelId || !targetId) return
    io.to(targetId).emit('voice:answer', { channelId, fromId: socket.id, sdp: payload?.sdp })
  })

  socket.on('voice:ice', (payload) => {
    const channelId = String(payload?.channelId || '')
    const targetId = String(payload?.targetId || '')
    if (!channelId || !targetId) return
    io.to(targetId).emit('voice:ice', { channelId, fromId: socket.id, candidate: payload?.candidate })
  })

  socket.on('disconnect', () => {
    voiceMembers.forEach((members, channelId) => {
      if (members.has(socket.id)) {
        members.delete(socket.id)
        emitVoiceMembers(channelId)
        io.to(`voice:${channelId}`).emit('voice:leave', { channelId, peerId: socket.id })
      }
    })
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
    channelId: 'ddnet-bridge',
    timestamp: timestamp || Date.now(),
    source: 'ddnet',
  }
  io.emit('chat:message', bridged)
  if (messagesCol) {
    const doc = { ...bridged, ts: bridged.timestamp }
    messagesCol.insertOne(doc).catch((e) => console.error('[mongo] insert failed', e?.message || e))
  } else {
    messageHistory.push(bridged)
    if (messageHistory.length > MESSAGE_HISTORY_LIMIT) messageHistory.shift()
  }
  res.sendStatus(204)
})
