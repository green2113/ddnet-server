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
import bcrypt from 'bcryptjs'
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

const DEFAULT_SERVER_NAME = process.env.DEFAULT_SERVER_NAME || 'Server'
let defaultServerId = null

const createDefaultChannels = (serverId, createdBy) => {
  const textCategoryId = generateSnowflakeId()
  const voiceCategoryId = generateSnowflakeId()
  return [
    {
      id: textCategoryId,
      serverId,
      name: '텍스트 채널',
      type: 'category',
      hidden: false,
      createdAt: Date.now(),
      createdBy,
      order: 0,
    },
    {
      id: generateSnowflakeId(),
      serverId,
      name: 'general',
      type: 'text',
      categoryId: textCategoryId,
      hidden: false,
      createdAt: Date.now(),
      createdBy,
      order: 1,
    },
    {
      id: voiceCategoryId,
      serverId,
      name: '음성 채널',
      type: 'category',
      hidden: false,
      createdAt: Date.now(),
      createdBy,
      order: 2,
    },
    {
      id: generateSnowflakeId(),
      serverId,
      name: 'voice',
      type: 'voice',
      categoryId: voiceCategoryId,
      hidden: false,
      createdAt: Date.now(),
      createdBy,
      order: 3,
    },
  ]
}

let servers = []
let memberships = []
let invites = []
let channels = []
let serverOrders = new Map()

const isServerMember = async (userId, serverId) => {
  if (!userId || !serverId) return false
  if (membershipsCol) {
    const row = await membershipsCol.findOne({ userId, serverId }, { projection: { _id: 0 } })
    return !!row
  }
  return memberships.some((m) => m.userId === userId && m.serverId === serverId)
}

const getGuestServerIds = (session) => {
  if (!session) return []
  const list = session.guestServers
  return Array.isArray(list) ? list.map((id) => String(id)) : []
}

const addGuestServer = (session, serverId) => {
  if (!session || !serverId) return
  const list = getGuestServerIds(session)
  if (!list.includes(serverId)) {
    list.push(serverId)
    session.guestServers = list
  }
}

const isSessionMember = async (user, serverId, session) => {
  if (!user || !serverId) return false
  if (user.isGuest) {
    return getGuestServerIds(session).includes(serverId)
  }
  return isServerMember(user.id, serverId)
}

const getMembership = async (userId, serverId) => {
  if (!userId || !serverId) return null
  if (membershipsCol) {
    return membershipsCol.findOne({ userId, serverId }, { projection: { _id: 0 } })
  }
  return memberships.find((m) => m.userId === userId && m.serverId === serverId) || null
}

const addMembership = async ({ userId, serverId, role }) => {
  const row = { userId, serverId, role, joinedAt: Date.now() }
  if (membershipsCol) {
    await membershipsCol.updateOne({ userId, serverId }, { $set: row }, { upsert: true })
    return
  }
  const idx = memberships.findIndex((m) => m.userId === userId && m.serverId === serverId)
  if (idx >= 0) {
    memberships[idx] = row
  } else {
    memberships.push(row)
  }
}

const listServersForUser = async (userId) => {
  if (!userId) return []
  if (membershipsCol) {
    const rows = await membershipsCol.find({ userId }, { projection: { _id: 0 } }).toArray()
    if (rows.length === 0 && defaultServerId) {
      await addMembership({ userId, serverId: defaultServerId, role: 'member' })
      return listServersForUser(userId)
    }
    const ids = rows.map((row) => row.serverId)
    if (!ids.length) return []
    return serversCol.find({ id: { $in: ids } }, { projection: { _id: 0 } }).toArray()
  }
  const joined = memberships.filter((m) => m.userId === userId).map((m) => m.serverId)
  if (!joined.length && defaultServerId) {
    memberships.push({ userId, serverId: defaultServerId, role: 'member', joinedAt: Date.now() })
    return listServersForUser(userId)
  }
  return servers.filter((server) => joined.includes(server.id))
}

const isServerAdmin = async (userId, serverId) => {
  const membership = await getMembership(userId, serverId)
  if (!membership) return false
  return membership.role === 'owner' || membership.role === 'admin'
}

const isServerOwner = async (userId, serverId) => {
  const membership = await getMembership(userId, serverId)
  if (!membership) return false
  return membership.role === 'owner'
}

const listAdmins = async (serverId) => {
  if (!serverId) return []
  if (membershipsCol) {
    const rows = await membershipsCol
      .find({ serverId, role: { $in: ['owner', 'admin'] } }, { projection: { _id: 0 } })
      .toArray()
    return rows.map((row) => row.userId)
  }
  return memberships.filter((m) => m.serverId === serverId && (m.role === 'owner' || m.role === 'admin')).map((m) => m.userId)
}

const getServerById = async (serverId) => {
  if (!serverId) return null
  if (serversCol) {
    return serversCol.findOne({ id: serverId }, { projection: { _id: 0 } })
  }
  return servers.find((server) => server.id === serverId) || null
}

const insertServer = async (server) => {
  if (serversCol) {
    await serversCol.insertOne(server)
    return
  }
  servers.push(server)
}

const INVITE_CODE_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
const INVITE_CODE_MIN = 7
const INVITE_CODE_MAX = 9

const generateInviteCode = () => {
  const length = INVITE_CODE_MIN + Math.floor(Math.random() * (INVITE_CODE_MAX - INVITE_CODE_MIN + 1))
  let code = ''
  for (let i = 0; i < length; i += 1) {
    code += INVITE_CODE_ALPHABET[Math.floor(Math.random() * INVITE_CODE_ALPHABET.length)]
  }
  return code
}

const getInviteByCode = async (code) => {
  if (!code) return null
  if (invitesCol) {
    return invitesCol.findOne({ code }, { projection: { _id: 0 } })
  }
  return invites.find((invite) => invite.code === code) || null
}

const insertInvite = async (invite) => {
  if (invitesCol) {
    await invitesCol.insertOne(invite)
    return
  }
  invites.push(invite)
}

const INVITE_TTL_MS = 7 * 24 * 60 * 60 * 1000

const isInviteExpired = (invite) => {
  if (!invite) return true
  if (!invite.expiresAt) return false
  return Date.now() > Number(invite.expiresAt)
}

async function listChannels(serverId) {
  if (channelsCol) {
    const rows = await channelsCol.find(serverId ? { serverId } : {}, { projection: { _id: 0 } }).toArray()
    return rows
      .map((row, index) => ({
        ...row,
        order: Number.isFinite(row.order) ? row.order : (Number.isFinite(row.createdAt) ? row.createdAt : index),
        type: row.type === 'voice' ? 'voice' : row.type === 'category' ? 'category' : 'text',
        categoryId: row.categoryId || null,
        hidden: !!row.hidden,
      }))
      .sort((a, b) => {
        if (a.order !== b.order) return a.order - b.order
        return (a.createdAt || 0) - (b.createdAt || 0)
      })
  }
  return channels
    .filter((channel) => (serverId ? channel.serverId === serverId : true))
    .map((channel, index) => ({
      ...channel,
      order: Number.isFinite(channel.order) ? channel.order : (Number.isFinite(channel.createdAt) ? channel.createdAt : index),
      type: channel.type === 'voice' ? 'voice' : channel.type === 'category' ? 'category' : 'text',
      categoryId: channel.categoryId || null,
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
let serversCol
let membershipsCol
let invitesCol
let usersCol
let serverOrdersCol
const users = []
async function initMongo() {
  const uri = process.env.MONGODB_URI
  if (!uri) return
  mongoClient = new MongoClient(uri)
  await mongoClient.connect()
  const db = mongoClient.db(process.env.MONGO_DB || 'ddnet')
  messagesCol = db.collection(process.env.MONGO_COLL || 'messages')
  await messagesCol.createIndex({ ts: 1 })
  channelsCol = db.collection(process.env.MONGO_CHANNELS_COLL || 'channels')
  try {
    const indexes = await channelsCol.indexes()
    const legacy = indexes.find((idx) => idx?.name === 'name_1')
    const legacyServerName = indexes.find(
      (idx) => idx?.key?.serverId === 1 && idx?.key?.name === 1 && !Object.prototype.hasOwnProperty.call(idx.key || {}, 'type')
    )
    if (legacy) {
      await channelsCol.dropIndex(legacy.name)
    }
    if (legacyServerName) {
      await channelsCol.dropIndex(legacyServerName.name)
    }
  } catch (e) {
    console.warn('[mongo] channels index cleanup skipped', e?.message || e)
  }
  await channelsCol.createIndex({ serverId: 1, name: 1, type: 1 }, { unique: true })
  serversCol = db.collection(process.env.MONGO_SERVERS_COLL || 'servers')
  await serversCol.createIndex({ id: 1 }, { unique: true })
  membershipsCol = db.collection(process.env.MONGO_MEMBERSHIPS_COLL || 'memberships')
  await membershipsCol.createIndex({ serverId: 1, userId: 1 }, { unique: true })
  invitesCol = db.collection(process.env.MONGO_INVITES_COLL || 'invites')
  await invitesCol.createIndex({ code: 1 }, { unique: true })
  usersCol = db.collection(process.env.MONGO_USERS_COLL || 'users')
  await usersCol.createIndex({ email: 1 }, { unique: true })
  serverOrdersCol = db.collection(process.env.MONGO_SERVER_ORDERS_COLL || 'server_orders')
  await serverOrdersCol.createIndex({ userId: 1 }, { unique: true })
  const serverCount = await serversCol.countDocuments()
  if (serverCount === 0) {
    const ownerId = DEFAULT_ADMIN_IDS[0] || 'system'
    const server = { id: generateSnowflakeId(), name: DEFAULT_SERVER_NAME, ownerId, createdAt: Date.now() }
    await serversCol.insertOne(server)
    defaultServerId = server.id
    const defaultChannels = createDefaultChannels(server.id, ownerId)
    await channelsCol.insertMany(defaultChannels)
    await membershipsCol.insertOne({ userId: ownerId, serverId: server.id, role: 'owner', joinedAt: Date.now() })
  } else {
    const first = await serversCol.find({}, { projection: { _id: 0 } }).sort({ createdAt: 1 }).limit(1).toArray()
    defaultServerId = first[0]?.id || null
  }
  if (defaultServerId) {
    await channelsCol.updateMany({ serverId: { $exists: false } }, { $set: { serverId: defaultServerId } })
  }
  console.log('[mongo] connected')
}
initMongo().catch((e) => console.error('[mongo] init failed', e?.message || e))
if (!process.env.MONGODB_URI) {
  const ownerId = DEFAULT_ADMIN_IDS[0] || 'system'
  const server = { id: generateSnowflakeId(), name: DEFAULT_SERVER_NAME, ownerId, createdAt: Date.now() }
  servers = [server]
  defaultServerId = server.id
  channels = createDefaultChannels(server.id, ownerId)
  memberships = [{ userId: ownerId, serverId: server.id, role: 'owner', joinedAt: Date.now() }]
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
app.use((req, _res, next) => {
  if (!req.user && req.session?.localUser) {
    req.user = req.session.localUser
  }
  next()
})

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

const normalizeEmail = (value) => String(value || '').trim().toLowerCase()
const normalizeHandle = (value) => String(value || '').trim().replace(/^@+/, '')
const isValidHandle = (value) => /^[A-Za-z0-9_]{1,15}$/.test(value)

const SNOWFLAKE_EPOCH = BigInt(Number(process.env.SNOWFLAKE_EPOCH || 1704067200000)) // 2024-01-01
const SNOWFLAKE_WORKER_ID = BigInt(Number(process.env.SNOWFLAKE_WORKER_ID || 1) & 0x3ff)
const SNOWFLAKE_SEQUENCE_MASK = (1n << 12n) - 1n
let snowflakeLastTs = 0n
let snowflakeSeq = 0n

const waitNextMs = (lastTs) => {
  let now = BigInt(Date.now()) - SNOWFLAKE_EPOCH
  while (now <= lastTs) {
    now = BigInt(Date.now()) - SNOWFLAKE_EPOCH
  }
  return now
}

const generateSnowflakeId = () => {
  let ts = BigInt(Date.now()) - SNOWFLAKE_EPOCH
  if (ts < 0n) ts = 0n
  if (ts === snowflakeLastTs) {
    snowflakeSeq = (snowflakeSeq + 1n) & SNOWFLAKE_SEQUENCE_MASK
    if (snowflakeSeq === 0n) {
      ts = waitNextMs(snowflakeLastTs)
    }
  } else {
    snowflakeSeq = 0n
  }
  snowflakeLastTs = ts
  const id = (ts << 22n) | (SNOWFLAKE_WORKER_ID << 12n) | snowflakeSeq
  return id.toString()
}

const toPublicUser = (user) => ({
  id: user.id,
  username: user.username,
  displayName: user.displayName,
  avatar: user.avatar || null,
  email: user.email,
  authType: user.authType || 'local',
})

const findUserByEmail = async (email) => {
  if (usersCol) {
    return usersCol.findOne({ email }, { projection: { _id: 0 } })
  }
  return users.find((user) => user.email === email) || null
}

const findUserByHandle = async (handle) => {
  const rawHandle = normalizeHandle(handle)
  if (!rawHandle) return null
  const handleWithAt = `@${rawHandle}`
  if (usersCol) {
    const existing = await usersCol.findOne(
      { username: { $in: [rawHandle, handleWithAt] } },
      { projection: { _id: 0 } },
    )
    if (!existing) return null
    return normalizeHandle(existing.username) === rawHandle ? existing : null
  }
  return users.find((user) => normalizeHandle(user.username) === rawHandle) || null
}

const insertUser = async (user) => {
  if (usersCol) {
    await usersCol.insertOne(user)
    return
  }
  users.push(user)
}

app.post('/auth/register', async (req, res) => {
  const email = normalizeEmail(req.body?.email)
  const password = String(req.body?.password || '')
  const rawHandle = normalizeHandle(req.body?.username)
  if (!email || !password || !rawHandle) {
    return res.status(400).json({ error: 'email, password, username required' })
  }
  if (!isValidHandle(rawHandle)) {
    return res.status(400).json({ error: 'username invalid' })
  }
  if (password.length < 6) {
    return res.status(400).json({ error: 'password too short' })
  }
  const handleTaken = await findUserByHandle(rawHandle)
  if (handleTaken) {
    return res.status(409).json({ error: 'username already used' })
  }
  const existing = await findUserByEmail(email)
  if (existing) {
    return res.status(409).json({ error: 'email already used' })
  }
  const passwordHash = await bcrypt.hash(password, 10)
  const handle = `@${rawHandle}`
  const user = {
    id: generateSnowflakeId(),
    email,
    username: handle,
    displayName: rawHandle,
    avatar: null,
    passwordHash,
    createdAt: Date.now(),
    authType: 'local',
  }
  try {
    await insertUser(user)
    req.session.localUser = toPublicUser(user)
    if (req.session?.guestUser) delete req.session.guestUser
    res.status(201).json(req.session.localUser)
  } catch (e) {
    console.error('[auth] register failed', e?.message || e)
    res.status(500).json({ error: 'failed to register' })
  }
})

app.get('/auth/username-check', async (req, res) => {
  const rawHandle = normalizeHandle(req.query?.username)
  if (!rawHandle || !isValidHandle(rawHandle)) {
    return res.json({ available: false })
  }
  const existing = await findUserByHandle(rawHandle)
  res.json({ available: !existing })
})

app.post('/auth/login', async (req, res) => {
  const email = normalizeEmail(req.body?.email)
  const password = String(req.body?.password || '')
  if (!email || !password) {
    return res.status(400).json({ error: 'email and password required' })
  }
  try {
    const user = await findUserByEmail(email)
    if (!user) return res.status(401).json({ error: 'invalid credentials' })
    const ok = await bcrypt.compare(password, user.passwordHash || '')
    if (!ok) return res.status(401).json({ error: 'invalid credentials' })
    req.session.localUser = toPublicUser(user)
    if (req.session?.guestUser) delete req.session.guestUser
    res.json(req.session.localUser)
  } catch (e) {
    console.error('[auth] login failed', e?.message || e)
    res.status(500).json({ error: 'failed to login' })
  }
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

app.get('/api/servers', async (req, res) => {
  const user = getSessionUser(req)
  if (!user) return res.status(401).json({ error: 'unauthorized' })
  try {
    if (user.isGuest) {
      const ids = getGuestServerIds(req.session)
      const rows = await Promise.all(ids.map((id) => getServerById(id)))
      return res.json(rows.filter(Boolean))
    }
    const list = await listServersForUser(user.id)
    res.json(list)
  } catch (e) {
    console.error('[servers] list failed', e?.message || e)
    res.status(500).json({ error: 'failed to load servers' })
  }
})

app.post('/api/servers', async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  const name = String(req.body?.name || '').trim()
  if (!name) return res.status(400).json({ error: 'name required' })
  const server = {
    id: generateSnowflakeId(),
    name: name.slice(0, 64),
    ownerId: user.id,
    createdAt: Date.now(),
  }
  try {
    await insertServer(server)
    await addMembership({ userId: user.id, serverId: server.id, role: 'owner' })
    const defaultChannels = createDefaultChannels(server.id, user.id)
    await Promise.all(defaultChannels.map((channel) => upsertChannel(channel)))
    io.emit('servers:update')
    res.status(201).json(server)
  } catch (e) {
    console.error('[servers] create failed', e?.message || e)
    res.status(500).json({ error: 'failed to create server' })
  }
})

app.get('/api/servers/:serverId', async (req, res) => {
  const user = getSessionUser(req)
  if (!user) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  const member = await isSessionMember(user, serverId, req.session)
  if (!member) return res.status(403).json({ error: 'forbidden' })
  const server = await getServerById(serverId)
  if (!server) return res.status(404).json({ error: 'server not found' })
  res.json(server)
})

app.get('/api/servers/order', async (req, res) => {
  const user = getSessionUser(req)
  if (!user) return res.status(401).json({ error: 'unauthorized' })
  try {
    if (user.isGuest) {
      const order = Array.isArray(req.session?.guestServerOrder) ? req.session.guestServerOrder : []
      return res.json(order)
    }
    if (serverOrdersCol) {
      const row = await serverOrdersCol.findOne({ userId: user.id }, { projection: { _id: 0 } })
      return res.json(Array.isArray(row?.order) ? row.order : [])
    }
    const order = serverOrders.get(user.id)
    res.json(Array.isArray(order) ? order : [])
  } catch (e) {
    console.error('[servers] order load failed', e?.message || e)
    res.status(500).json({ error: 'failed to load server order' })
  }
})

app.put('/api/servers/order', async (req, res) => {
  const user = getSessionUser(req)
  if (!user) return res.status(401).json({ error: 'unauthorized' })
  const orderedIds = Array.isArray(req.body?.orderedIds) ? req.body.orderedIds.map((id) => String(id)) : []
  try {
    if (user.isGuest) {
      req.session.guestServerOrder = orderedIds
      return res.json({ ok: true })
    }
    if (serverOrdersCol) {
      await serverOrdersCol.updateOne(
        { userId: user.id },
        { $set: { userId: user.id, order: orderedIds, updatedAt: Date.now() } },
        { upsert: true }
      )
    } else {
      serverOrders.set(user.id, orderedIds)
    }
    res.json({ ok: true })
  } catch (e) {
    console.error('[servers] order save failed', e?.message || e)
    res.status(500).json({ error: 'failed to save server order' })
  }
})

app.get('/api/servers/:serverId/admins', async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  const member = await isServerMember(user.id, serverId)
  if (!member) return res.status(403).json({ error: 'forbidden' })
  try {
    const admins = await listAdmins(serverId)
    res.json(admins)
  } catch (e) {
    console.error('[admins] list failed', e?.message || e)
    res.status(500).json({ error: 'failed to load admins' })
  }
})

app.post('/api/servers/:serverId/admins', async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  if (!await isServerOwner(user.id, serverId)) return res.status(403).json({ error: 'forbidden' })
  const id = String(req.body?.id || '').trim()
  if (!id) return res.status(400).json({ error: 'id required' })
  const server = await getServerById(serverId)
  if (!server) return res.status(404).json({ error: 'server not found' })
  try {
    await addMembership({ userId: id, serverId, role: 'admin' })
    const admins = await listAdmins(serverId)
    res.status(201).json(admins)
  } catch (e) {
    console.error('[admins] add failed', e?.message || e)
    res.status(500).json({ error: 'failed to add admin' })
  }
})

app.delete('/api/servers/:serverId/admins/:id', async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  if (!await isServerOwner(user.id, serverId)) return res.status(403).json({ error: 'forbidden' })
  const id = String(req.params.id || '').trim()
  if (!id) return res.status(400).json({ error: 'id required' })
  if (id === user.id) return res.status(400).json({ error: 'cannot remove owner' })
  try {
    const existing = await getMembership(id, serverId)
    if (!existing) return res.status(404).json({ error: 'member not found' })
    const role = existing.role === 'owner' ? 'owner' : 'member'
    await addMembership({ userId: id, serverId, role })
    const admins = await listAdmins(serverId)
    res.json(admins)
  } catch (e) {
    console.error('[admins] remove failed', e?.message || e)
    res.status(500).json({ error: 'failed to remove admin' })
  }
})

app.get('/api/servers/:serverId/channels', async (req, res) => {
  const user = getSessionUser(req)
  if (!user) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  const member = await isSessionMember(user, serverId, req.session)
  if (!member) return res.status(403).json({ error: 'forbidden' })
  try {
    let allChannels = await listChannels(serverId)
    if (!allChannels.some((channel) => channel.type === 'category')) {
      const textCategoryId = generateSnowflakeId()
      const voiceCategoryId = generateSnowflakeId()
      const now = Date.now()
      const categories = [
        {
          id: textCategoryId,
          serverId,
          name: '텍스트 채널',
          type: 'category',
          hidden: false,
          createdAt: now,
          createdBy: user.id,
          order: 0,
        },
        {
          id: voiceCategoryId,
          serverId,
          name: '음성 채널',
          type: 'category',
          hidden: false,
          createdAt: now + 1,
          createdBy: user.id,
          order: 1,
        },
      ]
      for (const category of categories) {
        await upsertChannel(category)
      }
      const updates = allChannels.map((channel, index) => {
        const categoryId = channel.type === 'voice' ? voiceCategoryId : textCategoryId
        const nextOrder = Number.isFinite(channel.order) ? channel.order : index
        return { ...channel, categoryId, order: nextOrder + 2 }
      })
      for (const channel of updates) {
        await upsertChannel(channel)
      }
      allChannels = await listChannels(serverId)
    }
    const isAdmin = await isServerAdmin(user.id, serverId)
    const visible = isAdmin ? allChannels : allChannels.filter((channel) => !channel.hidden)
    res.json(visible)
  } catch (e) {
    console.error('[channels] list failed', e?.message || e)
    res.status(500).json({ error: 'failed to load channels' })
  }
})

app.post('/api/servers/:serverId/channels', async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  if (!await isServerAdmin(user.id, serverId)) return res.status(403).json({ error: 'forbidden' })
  const name = String(req.body?.name || '').trim()
  if (!name) return res.status(400).json({ error: 'name required' })
  const type = req.body?.type === 'category' ? 'category' : req.body?.type === 'voice' ? 'voice' : 'text'
  const requestedCategoryId = req.body?.categoryId ? String(req.body.categoryId) : null
  const order = channelsCol
    ? await channelsCol.countDocuments({ serverId })
    : channels.filter((channel) => channel.serverId === serverId).length
  let categoryId = null
  if (type !== 'category' && requestedCategoryId) {
    const category = await getChannelById(requestedCategoryId)
    if (category && category.serverId === serverId && category.type === 'category') {
      categoryId = category.id
    }
  }
  const channel = {
    id: generateSnowflakeId(),
    serverId,
    name,
    type,
    categoryId,
    hidden: false,
    createdAt: Date.now(),
    createdBy: user.id,
    order,
  }
  try {
    await upsertChannel(channel)
    io.emit('channels:update', { serverId })
    res.status(201).json(channel)
  } catch (e) {
    console.error('[channels] create failed', e?.message || e)
    res.status(500).json({ error: 'failed to create channel' })
  }
})

app.delete('/api/servers/:serverId/channels/:id', async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  if (!await isServerAdmin(user.id, serverId)) return res.status(403).json({ error: 'forbidden' })
  const channelId = req.params.id
  try {
    const channel = await getChannelById(channelId)
    if (!channel || channel.serverId !== serverId) return res.status(404).json({ error: 'channel not found' })
    if (channel.type === 'category') {
      if (channelsCol) {
        await channelsCol.updateMany({ serverId, categoryId: channelId }, { $set: { categoryId: null } })
      } else {
        channels = channels.map((row) => (row.categoryId === channelId ? { ...row, categoryId: null } : row))
      }
    }
    await removeChannel(channelId)
    if (messagesCol) {
      await messagesCol.deleteMany({ channelId })
    } else {
      let idx = messageHistory.length
      while (idx--) {
        if (messageHistory[idx].channelId === channelId) messageHistory.splice(idx, 1)
      }
    }
    io.emit('channels:update', { serverId })
    res.sendStatus(204)
  } catch (e) {
    console.error('[channels] delete failed', e?.message || e)
    res.status(500).json({ error: 'failed to delete channel' })
  }
})

app.patch('/api/servers/:serverId/channels/:id/hidden', async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  if (!await isServerAdmin(user.id, serverId)) return res.status(403).json({ error: 'forbidden' })
  const channelId = req.params.id
  const hidden = Boolean(req.body?.hidden)
  try {
    const channel = await getChannelById(channelId)
    if (!channel || channel.serverId !== serverId) return res.status(404).json({ error: 'channel not found' })
    const updated = { ...channel, hidden }
    await upsertChannel(updated)
    io.emit('channels:update', { serverId })
    res.json(updated)
  } catch (e) {
    console.error('[channels] hide failed', e?.message || e)
    res.status(500).json({ error: 'failed to update channel' })
  }
})

app.patch('/api/servers/:serverId/channels/:id/name', async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  if (!await isServerAdmin(user.id, serverId)) return res.status(403).json({ error: 'forbidden' })
  const channelId = req.params.id
  const name = String(req.body?.name || '').trim()
  if (!name) return res.status(400).json({ error: 'name required' })
  try {
    const channel = await getChannelById(channelId)
    if (!channel || channel.serverId !== serverId) return res.status(404).json({ error: 'channel not found' })
    const updated = { ...channel, name: name.slice(0, 64) }
    await upsertChannel(updated)
    io.emit('channels:update', { serverId })
    res.json(updated)
  } catch (e) {
    console.error('[channels] rename failed', e?.message || e)
    res.status(500).json({ error: 'failed to rename channel' })
  }
})

app.patch('/api/servers/:serverId/channels/order', async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  if (!await isServerAdmin(user.id, serverId)) return res.status(403).json({ error: 'forbidden' })
  const orderedIds = Array.isArray(req.body?.orderedIds) ? req.body.orderedIds.map((id) => String(id)) : []
  if (orderedIds.length === 0) return res.status(400).json({ error: 'orderedIds required' })
  const categoryId = req.body?.categoryId ? String(req.body.categoryId) : null
  try {
    if (channelsCol) {
      const rows = await channelsCol.find({ serverId }, { projection: { _id: 0 } }).toArray()
      const targetRows = rows.filter((row) => row.type !== 'category' && (categoryId ? row.categoryId === categoryId : !row.categoryId))
      const channelMap = new Map(rows.filter((row) => row.type !== 'category').map((row) => [row.id, row]))
      const used = new Set()
      let order = 0
      const updates = []
      orderedIds.forEach((id) => {
        if (!channelMap.has(id)) return
        used.add(id)
        updates.push({ id, order: order++, categoryId: categoryId || null })
      })
      targetRows.forEach((row) => {
        if (used.has(row.id)) return
        updates.push({ id: row.id, order: order++ })
      })
      await Promise.all(
        updates.map((update) =>
          channelsCol.updateOne(
            { id: update.id },
            { $set: { order: update.order, ...(update.categoryId !== undefined ? { categoryId: update.categoryId } : {}) } },
          )
        )
      )
    } else {
      const serverChannels = channels.filter((channel) => channel.serverId === serverId)
      const targetChannels = serverChannels.filter((channel) => channel.type !== 'category' && (categoryId ? channel.categoryId === categoryId : !channel.categoryId))
      const channelMap = new Map(serverChannels.filter((channel) => channel.type !== 'category').map((channel) => [channel.id, channel]))
      const used = new Set()
      let order = 0
      const ordered = []
      orderedIds.forEach((id) => {
        const channel = channelMap.get(id)
        if (!channel) return
        used.add(id)
        ordered.push({ ...channel, order: order++, categoryId: categoryId || null })
      })
      targetChannels.forEach((channel) => {
        if (used.has(channel.id)) return
        ordered.push({ ...channel, order: order++ })
      })
      channels = channels.map((channel) => {
        const next = ordered.find((item) => item.id === channel.id)
        return next || channel
      })
    }
    io.emit('channels:update', { serverId })
    res.json({ ok: true })
  } catch (e) {
    console.error('[channels] order failed', e?.message || e)
    res.status(500).json({ error: 'failed to reorder channels' })
  }
})

app.patch('/api/servers/:serverId/categories/order', async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  if (!await isServerAdmin(user.id, serverId)) return res.status(403).json({ error: 'forbidden' })
  const orderedIds = Array.isArray(req.body?.orderedIds) ? req.body.orderedIds.map((id) => String(id)) : []
  if (orderedIds.length === 0) return res.status(400).json({ error: 'orderedIds required' })
  try {
    if (channelsCol) {
      const rows = await channelsCol.find({ serverId, type: 'category' }, { projection: { _id: 0 } }).toArray()
      const categoryMap = new Map(rows.map((row) => [row.id, row]))
      const used = new Set()
      let order = 0
      const updates = []
      orderedIds.forEach((id) => {
        if (!categoryMap.has(id)) return
        used.add(id)
        updates.push({ id, order: order++ })
      })
      rows.forEach((row) => {
        if (used.has(row.id)) return
        updates.push({ id: row.id, order: order++ })
      })
      await Promise.all(updates.map((update) => channelsCol.updateOne({ id: update.id }, { $set: { order: update.order } })))
    } else {
      const serverChannels = channels.filter((channel) => channel.serverId === serverId && channel.type === 'category')
      const categoryMap = new Map(serverChannels.map((channel) => [channel.id, channel]))
      const used = new Set()
      let order = 0
      const ordered = []
      orderedIds.forEach((id) => {
        const category = categoryMap.get(id)
        if (!category) return
        used.add(id)
        ordered.push({ ...category, order: order++ })
      })
      serverChannels.forEach((category) => {
        if (used.has(category.id)) return
        ordered.push({ ...category, order: order++ })
      })
      channels = channels.map((channel) => {
        const next = ordered.find((item) => item.id === channel.id)
        return next || channel
      })
    }
    io.emit('channels:update', { serverId })
    res.json({ ok: true })
  } catch (e) {
    console.error('[categories] order failed', e?.message || e)
    res.status(500).json({ error: 'failed to reorder categories' })
  }
})

app.post('/api/servers/:serverId/invites', async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  const serverId = String(req.params.serverId || '')
  if (!serverId) return res.status(400).json({ error: 'serverId required' })
  const member = await isServerMember(user.id, serverId)
  if (!member) return res.status(403).json({ error: 'forbidden' })
  const server = await getServerById(serverId)
  if (!server) return res.status(404).json({ error: 'server not found' })
  let code = null
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const next = generateInviteCode()
    const exists = await getInviteByCode(next)
    if (!exists) {
      code = next
      break
    }
  }
  if (!code) return res.status(500).json({ error: 'failed to generate invite' })
  const invite = {
    code,
    serverId,
    createdBy: user.id,
    createdAt: Date.now(),
    expiresAt: Date.now() + INVITE_TTL_MS,
  }
  try {
    await insertInvite(invite)
    const base = (ORIGIN || '').replace(/\/$/, '')
    res.status(201).json({ ...invite, url: base ? `${base}/invite/${code}` : `/invite/${code}` })
  } catch (e) {
    console.error('[invites] create failed', e?.message || e)
    res.status(500).json({ error: 'failed to create invite' })
  }
})

app.get('/api/invite/:code', async (req, res) => {
  const code = String(req.params.code || '').trim()
  if (!code) return res.status(400).json({ error: 'code required' })
  const invite = await getInviteByCode(code)
  if (!invite) return res.status(404).json({ error: 'invite not found' })
  if (isInviteExpired(invite)) return res.status(410).json({ error: 'invite expired' })
  const server = await getServerById(invite.serverId)
  if (!server) return res.status(404).json({ error: 'server not found' })
  res.json({ code: invite.code, server: { id: server.id, name: server.name, ownerId: server.ownerId } })
})

app.post('/api/invite/:code/join', async (req, res) => {
  const user = getSessionUser(req)
  if (!user) return res.status(401).json({ error: 'unauthorized' })
  const code = String(req.params.code || '').trim()
  if (!code) return res.status(400).json({ error: 'code required' })
  const invite = await getInviteByCode(code)
  if (!invite) return res.status(404).json({ error: 'invite not found' })
  if (isInviteExpired(invite)) return res.status(410).json({ error: 'invite expired' })
  const server = await getServerById(invite.serverId)
  if (!server) return res.status(404).json({ error: 'server not found' })
  if (user.isGuest) {
    addGuestServer(req.session, server.id)
  } else {
    const member = await isServerMember(user.id, server.id)
    if (!member) {
      await addMembership({ userId: user.id, serverId: server.id, role: 'member' })
    }
  }
  res.json({ server })
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
  const user = getSessionUser(req)
  if (!user) return res.status(401).json({ error: 'unauthorized' })
  const limit = Math.min(Number(req.query.limit) || 200, MESSAGE_HISTORY_LIMIT)
  const channelId = String(req.query.channelId || 'general')
  const channel = await getChannelById(channelId)
  if (!channel) return res.status(404).json({ error: 'channel not found' })
  const member = await isSessionMember(user, channel.serverId, req.session)
  if (!member) return res.status(403).json({ error: 'forbidden' })
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


app.post('/api/upload', upload.single('file'), async (req, res) => {
  const user = getSessionUser(req)
  if (!user || user.isGuest) return res.status(401).json({ error: 'unauthorized' })
  if (!r2Client || !R2_BUCKET || !R2_PUBLIC_BASE_URL) {
    return res.status(500).json({ error: 'upload not configured' })
  }
  const channelId = String(req.body?.channelId || '')
  if (!channelId) return res.status(400).json({ error: 'channelId required' })
  const channel = await getChannelById(channelId)
  if (!channel) return res.status(404).json({ error: 'channel not found' })
  const member = await isServerMember(user.id, channel.serverId)
  if (!member) return res.status(403).json({ error: 'forbidden' })
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
    return sess?.passport?.user || sess?.localUser || sess?.guestUser
  }

  socket.on('voice:watch', async (payload) => {
    const channelId = String(payload?.channelId || '')
    if (!channelId) return
    const channel = await getChannelById(channelId)
    if (!channel || channel.type !== 'voice') return
    const sessUser = resolveSessionUser()
    if (!sessUser) return
    const member = await isSessionMember(sessUser, channel.serverId, socket.request?.session)
    if (!member) return
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
    const member = await isSessionMember(sessUser, channel.serverId, socket.request?.session)
    if (!member) return
    if (channel.hidden && !await isServerAdmin(sessUser?.id, channel.serverId)) return
    const message = {
      id: generateSnowflakeId(),
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
      let deleted = false
      let target = null
      if (messagesCol) {
        target = await messagesCol.findOne({ id: messageId }, { projection: { author: 1, user_id: 1, channelId: 1 } })
      } else {
        target = messageHistory.find((m) => m.id === messageId) || null
      }
      if (!target?.channelId) return
      const channel = await getChannelById(target.channelId)
      if (!channel) return
      const member = await isServerMember(sessUser.id, channel.serverId)
      if (!member) return
      const canDeleteAny = await isServerAdmin(sessUser.id, channel.serverId)
      const authorId = target?.author?.id || target?.user_id
      if (!authorId || (authorId !== sessUser.id && !canDeleteAny)) return
      if (messagesCol) {
        const r = await messagesCol.deleteOne({ id: messageId })
        deleted = r.deletedCount > 0
      } else {
        const idx = messageHistory.findIndex((m) => m.id === messageId)
        if (idx >= 0) {
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
    const member = await isSessionMember(sessUser, channel.serverId, socket.request?.session)
    if (!member) return
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
    id: generateSnowflakeId(),
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
