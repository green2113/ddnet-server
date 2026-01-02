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
import multer from 'multer'
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'

const app = express()
const DEFAULT_ADMIN_IDS = ['776421522188664843']
const ORIGIN = process.env.WEB_ORIGIN || ''
if (!ORIGIN) {
  console.warn('[WARN] WEB_ORIGIN is not set. Falling back to http://localhost:5173 for redirects.')
}
const corsOrigins = (process.env.CORS_ORIGINS || ORIGIN || 'http://localhost:5173')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean)
const allowAllOrigins = corsOrigins.includes('*')
const allowNullOrigin = corsOrigins.includes('null')
const allowFileOrigin = corsOrigins.includes('file://') || corsOrigins.includes('file')
const allowAppOrigin = corsOrigins.includes('app://')
const corsOriginSet = new Set(corsOrigins)

const corsOriginHandler = (origin, callback) => {
  if (allowAllOrigins) return callback(null, true)
  if (!origin) return callback(null, true)
  if (origin === 'null' && allowNullOrigin) return callback(null, true)
  if (origin.startsWith('file://') && (allowNullOrigin || allowFileOrigin)) return callback(null, true)
  if (origin.startsWith('app://') && allowAppOrigin) return callback(null, true)
  if (corsOriginSet.has(origin)) return callback(null, true)
  console.warn(`[cors] blocked origin: ${origin}`)
  return callback(new Error(`Not allowed by CORS: ${origin}`))
}
const httpServer = createServer(app)
const io = new SocketIOServer(httpServer, {
  cors: {
    origin: corsOriginHandler,
    credentials: true,
  },
})

app.use(cors({ origin: corsOriginHandler, credentials: true }))
app.use(express.json())
app.use(cookieParser())
// trust proxy for correct secure cookies behind Fly/Proxies
app.set('trust proxy', 1)

// Message history in memory (fallback when MongoDB is not configured)
const MESSAGE_HISTORY_LIMIT = Number(process.env.MESSAGE_HISTORY_LIMIT || 500)
const messageHistory = []

const defaultChannels = () => [
  { id: '1000', name: 'general', type: 'text', hidden: false, createdAt: Date.now(), createdBy: 'system', order: 0 },
  { id: '1001', name: 'ddnet-bridge', type: 'text', hidden: false, createdAt: Date.now(), createdBy: 'system', order: 1 },
]

let channels = defaultChannels()
let adminIds = [...DEFAULT_ADMIN_IDS]

const isAdminId = (userId) => Boolean(userId) && adminIds.includes(userId)

const generateChannelId = () => `${Date.now()}${Math.floor(Math.random() * 9000 + 1000)}`

async function listChannels() {
  if (channelsCol) {
    const rows = await channelsCol.find({}, { projection: { _id: 0 } }).toArray()
    return rows
      .map((row, index) => ({
        ...row,
        order: Number.isFinite(row.order) ? row.order : (Number.isFinite(row.createdAt) ? row.createdAt : index),
        type: row.type === 'voice' ? 'voice' : 'text',
        hidden: !!row.hidden,
      }))
      .sort((a, b) => {
        if (a.order !== b.order) return a.order - b.order
        return (a.createdAt || 0) - (b.createdAt || 0)
      })
  }
  return channels
    .map((channel, index) => ({
      ...channel,
      order: Number.isFinite(channel.order) ? channel.order : (Number.isFinite(channel.createdAt) ? channel.createdAt : index),
      type: channel.type === 'voice' ? 'voice' : 'text',
    }))
    .sort((a, b) => {
      if (a.order !== b.order) return a.order - b.order
      return (a.createdAt || 0) - (b.createdAt || 0)
    })
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
let adminsCol
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
  adminsCol = db.collection(process.env.MONGO_ADMINS_COLL || 'admins')
  await adminsCol.createIndex({ id: 1 }, { unique: true })
  const existing = await channelsCol.countDocuments()
  if (existing === 0) {
    await channelsCol.insertMany(defaultChannels())
  }
  const adminCount = await adminsCol.countDocuments()
  if (adminCount === 0) {
    await adminsCol.insertMany(DEFAULT_ADMIN_IDS.map((id) => ({ id })))
  }
  console.log('[mongo] connected')
}
initMongo().catch((e) => console.error('[mongo] init failed', e?.message || e))

async function listAdmins() {
  if (adminsCol) {
    const rows = await adminsCol.find({}, { projection: { _id: 0 } }).toArray()
    adminIds = rows.map((row) => row.id)
    return adminIds
  }
  return adminIds
}

async function addAdmin(id) {
  if (adminsCol) {
    await adminsCol.updateOne({ id }, { $set: { id } }, { upsert: true })
  } else if (!adminIds.includes(id)) {
    adminIds.push(id)
  }
  return listAdmins()
}

async function removeAdmin(id) {
  if (adminsCol) {
    await adminsCol.deleteOne({ id })
  } else {
    adminIds = adminIds.filter((existing) => existing !== id)
  }
  return listAdmins()
}

const isHttps = (ORIGIN || '').startsWith('https://')
const sessionMiddleware = session({
  secret: process.env.SESSION_SECRET || 'dev_secret_change_me',
  resave: false,
  saveUninitialized: false,
  cookie: { secure: isHttps, sameSite: isHttps ? 'none' : 'lax' },
})
app.use(sessionMiddleware)

const DEFAULT_GUEST_AVATAR = 'https://cdn.discordapp.com/embed/avatars/0.png'
const UPLOAD_LIMIT_BYTES = 50 * 1024 * 1024
const ALLOWED_MIME_TYPES = new Set([
  'image/png',
  'image/jpeg',
  'image/webp',
  'image/gif',
  'application/pdf',
  'text/plain',
  'application/zip',
])
const R2_ENDPOINT = process.env.R2_ENDPOINT || ''
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID || ''
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY || ''
const R2_BUCKET = process.env.R2_BUCKET || ''
const R2_REGION = process.env.R2_REGION || 'auto'
const R2_PUBLIC_BASE_URL = process.env.R2_PUBLIC_BASE_URL || ''

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: UPLOAD_LIMIT_BYTES },
})

const getSessionUser = (req) => req.user || req.session?.guestUser || null

const r2Client = R2_ENDPOINT && R2_ACCESS_KEY_ID && R2_SECRET_ACCESS_KEY
  ? new S3Client({
      region: R2_REGION,
      endpoint: R2_ENDPOINT,
      credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_ACCESS_KEY },
      forcePathStyle: true,
    })
  : null

const safeFilename = (value) => {
  const trimmed = String(value || '').trim().replace(/[\\/]/g, '_')
  const cleaned = trimmed.replace(/[^\w.\-()\s]/g, '_')
  return cleaned || 'file'
}

const buildPublicUrl = (key) => {
  const base = String(R2_PUBLIC_BASE_URL || '').replace(/\/$/, '')
  if (!base) return ''
  const encoded = key.split('/').map((part) => encodeURIComponent(part)).join('/')
  return `${base}/${encoded}`
}

const generateAttachmentId = () => {
  const now = Date.now().toString()
  const rand = Math.floor(Math.random() * 1_000_000).toString().padStart(6, '0')
  return `${now}${rand}`
}

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

const sanitizeReturnTo = (value) => {
  if (typeof value !== 'string') return null
  if (!value.startsWith('/') || value.startsWith('//')) return null
  return value
}

app.get(
  '/auth/discord',
  (req, _res, next) => {
    if (req.session?.guestUser) delete req.session.guestUser
    const returnTo = sanitizeReturnTo(req.query?.return_to)
    if (returnTo) {
      req.session.returnTo = returnTo
    }
    next()
  },
  passport.authenticate('discord'),
)
app.get(
  '/auth/discord/callback',
  passport.authenticate('discord', {
    failureRedirect: `${ORIGIN || 'http://localhost:5173'}/`,
  }),
  (req, res) => {
    // 로그인 성공: 프런트엔드로 복귀
    const base = ORIGIN || 'http://localhost:5173'
    const returnTo = sanitizeReturnTo(req.session?.returnTo) || '/'
    if (req.session?.returnTo) delete req.session.returnTo
    res.redirect(`${base.replace(/\/$/, '')}${returnTo}`)
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
  if (req.user) return res.json({ ...req.user, isGuest: false })
  if (req.session?.guestUser) return res.json(req.session.guestUser)
  res.json(null)
})

app.post('/auth/guest', (req, res) => {
  const name = String(req.body?.name || '').trim()
  if (!name) return res.status(400).json({ error: 'name required' })
  const displayName = name.slice(0, 20)
  const guestUser = {
    id: req.session?.guestUser?.id || `guest:${randomUUID()}`,
    username: displayName,
    displayName,
    avatar: DEFAULT_GUEST_AVATAR,
    isGuest: true,
  }
  req.session.guestUser = guestUser
  res.status(201).json(guestUser)
})

app.get('/api/admins', async (req, res) => {
  try {
    const admins = await listAdmins()
    res.json(admins)
  } catch (e) {
    console.error('[admins] list failed', e?.message || e)
    res.status(500).json({ error: 'failed to load admins' })
  }
})

app.post('/api/admins', async (req, res) => {
  if (!isAdminId(req.user?.id)) return res.status(403).json({ error: 'forbidden' })
  const id = String(req.body?.id || '').trim()
  if (!id) return res.status(400).json({ error: 'id required' })
  try {
    const admins = await addAdmin(id)
    res.status(201).json(admins)
  } catch (e) {
    console.error('[admins] add failed', e?.message || e)
    res.status(500).json({ error: 'failed to add admin' })
  }
})

app.delete('/api/admins/:id', async (req, res) => {
  if (!isAdminId(req.user?.id)) return res.status(403).json({ error: 'forbidden' })
  const id = String(req.params.id || '').trim()
  if (!id) return res.status(400).json({ error: 'id required' })
  try {
    const admins = await removeAdmin(id)
    res.json(admins)
  } catch (e) {
    console.error('[admins] remove failed', e?.message || e)
    res.status(500).json({ error: 'failed to remove admin' })
  }
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
  const order = channelsCol ? await channelsCol.countDocuments() : channels.length
  const channel = {
    id: generateChannelId(),
    name,
    type,
    hidden: false,
    createdAt: Date.now(),
    createdBy: userId,
    order,
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

app.patch('/api/channels/order', async (req, res) => {
  const userId = req.user?.id
  if (!isAdminId(userId)) return res.status(403).json({ error: 'forbidden' })
  const orderedIds = Array.isArray(req.body?.orderedIds) ? req.body.orderedIds.map((id) => String(id)) : []
  if (orderedIds.length === 0) return res.status(400).json({ error: 'orderedIds required' })
  try {
    if (channelsCol) {
      const rows = await channelsCol.find({}, { projection: { _id: 0 } }).toArray()
      const channelMap = new Map(rows.map((row) => [row.id, row]))
      const used = new Set()
      let order = 0
      const updates = []
      orderedIds.forEach((id) => {
        if (!channelMap.has(id)) return
        used.add(id)
        updates.push({ id, order: order++ })
      })
      rows.forEach((row) => {
        if (used.has(row.id)) return
        updates.push({ id: row.id, order: order++ })
      })
      await Promise.all(updates.map((update) => channelsCol.updateOne({ id: update.id }, { $set: { order: update.order } })))
    } else {
      const channelMap = new Map(channels.map((channel) => [channel.id, channel]))
      const used = new Set()
      let order = 0
      const ordered = []
      orderedIds.forEach((id) => {
        const channel = channelMap.get(id)
        if (!channel) return
        used.add(id)
        ordered.push({ ...channel, order: order++ })
      })
      channels.forEach((channel) => {
        if (used.has(channel.id)) return
        ordered.push({ ...channel, order: order++ })
      })
      channels = ordered
    }
    io.emit('channels:update')
    res.json({ ok: true })
  } catch (e) {
    console.error('[channels] order failed', e?.message || e)
    res.status(500).json({ error: 'failed to reorder channels' })
  }
})

app.post('/api/upload', upload.single('file'), async (req, res) => {
  const user = getSessionUser(req)
  if (!user) return res.status(401).json({ error: 'unauthorized' })
  if (!r2Client || !R2_BUCKET || !R2_PUBLIC_BASE_URL) {
    return res.status(500).json({ error: 'upload not configured' })
  }
  const channelId = String(req.body?.channelId || '')
  if (!channelId) return res.status(400).json({ error: 'channelId required' })
  const channel = await getChannelById(channelId)
  if (!channel) return res.status(404).json({ error: 'channel not found' })
  const file = req.file
  if (!file) return res.status(400).json({ error: 'file required' })
  if (!ALLOWED_MIME_TYPES.has(file.mimetype)) {
    return res.status(400).json({ error: 'file type not allowed' })
  }

  const filename = safeFilename(file.originalname)
  const attachmentId = generateAttachmentId()
  const key = `attachments/${channelId}/${attachmentId}/${filename}`
  try {
    await r2Client.send(
      new PutObjectCommand({
        Bucket: R2_BUCKET,
        Key: key,
        Body: file.buffer,
        ContentType: file.mimetype || 'application/octet-stream',
      }),
    )
    const url = buildPublicUrl(key)
    res.status(201).json({ url, key, size: file.size, mime: file.mimetype || 'application/octet-stream' })
  } catch (err) {
    console.error('[upload] failed', err?.message || err)
    res.status(500).json({ error: 'upload failed' })
  }
})

// (Discord 봇 미사용) 디스코드 채널 브릿지는 제거되었습니다.

io.engine.use(sessionMiddleware)
io.use((socket, next) => {
  next()
})

const voiceMembers = new Map()
const voiceMembersByUser = new Map()

const emitVoiceMembers = (channelId) => {
  const members = Array.from(voiceMembers.get(channelId)?.values() || [])
  io.to(`voice:${channelId}`).emit('voice:members', { channelId, members })
  io.to(`voice:watch:${channelId}`).emit('voice:members', { channelId, members })
}

io.on('connection', (socket) => {
  const resolveSessionUser = () => {
    const sess = socket.request?.session
    return sess?.passport?.user || sess?.guestUser
  }

  socket.on('voice:watch', async (payload) => {
    const channelId = String(payload?.channelId || '')
    if (!channelId) return
    const channel = await getChannelById(channelId)
    if (!channel || channel.type !== 'voice') return
    socket.join(`voice:watch:${channelId}`)
    emitVoiceMembers(channelId)
  })

  socket.on('voice:unwatch', (payload) => {
    const channelId = String(payload?.channelId || '')
    if (!channelId) return
    socket.leave(`voice:watch:${channelId}`)
  })

  socket.on('chat:send', async (payload) => {
    const sessUser = resolveSessionUser()
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
      const sessUser = resolveSessionUser()
      if (!sessUser) return
      const canDeleteAny = isAdminId(sessUser.id)

      let deleted = false
      if (messagesCol) {
        // Find the message to verify ownership
        const found = await messagesCol.findOne({ id: messageId }, { projection: { author: 1, user_id: 1 } })
        const authorId = found?.author?.id || found?.user_id
        if (authorId && (authorId === sessUser.id || canDeleteAny)) {
          const r = await messagesCol.deleteOne({ id: messageId })
          deleted = r.deletedCount > 0
        }
      } else {
        const idx = messageHistory.findIndex((m) => m.id === messageId)
        if (idx >= 0 && (messageHistory[idx].author?.id === sessUser.id || canDeleteAny)) {
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
    const sessUser = resolveSessionUser()
    if (!sessUser) return
    const channelId = String(payload?.channelId || '')
    if (!channelId) return
    const channel = await getChannelById(channelId)
    if (!channel || channel.type !== 'voice') return
    voiceMembers.forEach((members, existingChannelId) => {
      if (existingChannelId === channelId) return
      if (!members.has(socket.id)) return
      members.delete(socket.id)
      if (members.size === 0) {
        voiceMembers.delete(existingChannelId)
      }
      socket.leave(`voice:${existingChannelId}`)
      emitVoiceMembers(existingChannelId)
      io.to(`voice:${existingChannelId}`).emit('voice:leave', { channelId: existingChannelId, peerId: socket.id })
    })
    if (!voiceMembers.has(channelId)) {
      voiceMembers.set(channelId, new Map())
    }
    if (!voiceMembersByUser.has(channelId)) {
      voiceMembersByUser.set(channelId, new Map())
    }
    const channelMembers = voiceMembers.get(channelId)
    const channelMembersByUser = voiceMembersByUser.get(channelId)
    const userId = sessUser.id
    if (userId) {
      const existingSocketId = channelMembersByUser.get(userId)
      if (existingSocketId && existingSocketId !== socket.id) {
        const existingSocket = io.sockets.sockets.get(existingSocketId)
        if (channelMembers?.has(existingSocketId)) {
          channelMembers.delete(existingSocketId)
          io.to(`voice:${channelId}`).emit('voice:leave', { channelId, peerId: existingSocketId })
        }
        existingSocket?.leave(`voice:${channelId}`)
        io.to(existingSocketId).emit('voice:force-leave', { channelId })
      }
      channelMembersByUser.set(userId, socket.id)
    }
    channelMembers.set(socket.id, {
      id: socket.id,
      username: sessUser.username,
      displayName: sessUser.displayName,
      avatar: sessUser.avatar || null,
      muted: Boolean(payload?.muted),
      deafened: Boolean(payload?.deafened),
    })
    socket.join(`voice:${channelId}`)
    emitVoiceMembers(channelId)
  })

  socket.on('voice:leave', (payload) => {
    const channelId = String(payload?.channelId || '')
    if (!channelId) return
    const channelMembers = voiceMembers.get(channelId)
    const channelMembersByUser = voiceMembersByUser.get(channelId)
    if (channelMembers?.has(socket.id)) {
      channelMembers.delete(socket.id)
      if (channelMembers.size === 0) {
        voiceMembers.delete(channelId)
        voiceMembersByUser.delete(channelId)
      }
      socket.leave(`voice:${channelId}`)
      emitVoiceMembers(channelId)
      io.to(`voice:${channelId}`).emit('voice:leave', { channelId, peerId: socket.id })
    }
    if (channelMembersByUser) {
      for (const [userId, socketId] of channelMembersByUser.entries()) {
        if (socketId === socket.id) {
          channelMembersByUser.delete(userId)
          break
        }
      }
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

  socket.on('voice:status', (payload) => {
    const channelId = String(payload?.channelId || '')
    if (!channelId) return
    const channelMembers = voiceMembers.get(channelId)
    if (!channelMembers?.has(socket.id)) return
    const current = channelMembers.get(socket.id)
    channelMembers.set(socket.id, {
      ...current,
      muted: Boolean(payload?.muted),
      deafened: Boolean(payload?.deafened),
    })
    emitVoiceMembers(channelId)
  })

  socket.on('disconnect', () => {
    voiceMembers.forEach((members, channelId) => {
      if (members.has(socket.id)) {
        members.delete(socket.id)
        if (members.size === 0) {
          voiceMembers.delete(channelId)
          voiceMembersByUser.delete(channelId)
        }
        emitVoiceMembers(channelId)
        io.to(`voice:${channelId}`).emit('voice:leave', { channelId, peerId: socket.id })
      }
      const channelMembersByUser = voiceMembersByUser.get(channelId)
      if (channelMembersByUser) {
        for (const [userId, socketId] of channelMembersByUser.entries()) {
          if (socketId === socket.id) {
            channelMembersByUser.delete(userId)
            break
          }
        }
      }
    })
  })
})

const port = Number(process.env.PORT || 4000)
const host = process.env.HOST || '0.0.0.0'
httpServer.listen(port, host, () => {
  console.log(`Server listening on http://${host}:${port}`)
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
