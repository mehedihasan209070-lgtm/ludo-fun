import express from 'express';
import { createServer as createHttpServer } from 'http';
import { WebSocket, WebSocketServer } from 'ws';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = parseInt(process.env.PORT || '3000', 10);
const distPath = path.resolve(__dirname, 'dist');
const hasDist = fs.existsSync(path.resolve(distPath, 'index.html'));
const isProduction =
  process.env.NODE_ENV === 'production' ||
  (process.env.NODE_ENV !== 'development' && hasDist);

const app = express();
const httpServer = createHttpServer(app);

// WebSocket Server attached to same HTTP server
const wss = new WebSocketServer({ server: httpServer });

wss.on('error', (err) => {
  console.error('WebSocketServer global error:', err);
});

interface ConnectedPlayer {
  id: string;
  name: string;
  color: 'RED' | 'GREEN' | 'YELLOW' | 'BLUE';
  isHost: boolean;
  ws: WebSocket;
}

interface Room {
  code: string;
  playerCount: 2 | 3 | 4;
  players: Map<string, ConnectedPlayer>;
  gameState: any | null;
  createdAt: number;
}

const rooms = new Map<string, Room>();

const COLOR_ORDER_MAP: Record<2 | 3 | 4, Array<'RED' | 'GREEN' | 'YELLOW' | 'BLUE'>> = {
  2: ['RED', 'YELLOW'],
  3: ['RED', 'GREEN', 'YELLOW'],
  4: ['RED', 'GREEN', 'YELLOW', 'BLUE'],
};

wss.on('connection', (ws) => {
  let currentRoomCode: string | null = null;
  let currentPlayerId: string | null = null;

  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw.toString());
      const { type } = data;

      if (type === 'create_or_join_room') {
        const { roomCode, playerName, playerCount = 4 } = data;
        const normalizedCode = (roomCode || '').toUpperCase().trim();
        if (!normalizedCode) return;

        let room = rooms.get(normalizedCode);
        const playerId = 'p_' + Math.random().toString(36).substring(2, 9);
        currentPlayerId = playerId;
        currentRoomCode = normalizedCode;

        if (!room) {
          // Create new room
          room = {
            code: normalizedCode,
            playerCount: (playerCount === 2 || playerCount === 3 ? playerCount : 4) as 2 | 3 | 4,
            players: new Map(),
            gameState: null,
            createdAt: Date.now(),
          };
          rooms.set(normalizedCode, room);
        }

        const allowedColors = COLOR_ORDER_MAP[room.playerCount];
        const usedColors = new Set(Array.from(room.players.values()).map((p) => p.color));
        const availableColor = allowedColors.find((c) => !usedColors.has(c)) || allowedColors[0];

        const isHost = room.players.size === 0;
        const newPlayer: ConnectedPlayer = {
          id: playerId,
          name: playerName?.trim() || `Player ${room.players.size + 1}`,
          color: availableColor,
          isHost,
          ws,
        };

        room.players.set(playerId, newPlayer);

        // Notify the joined player
        const playersList = Array.from(room.players.values()).map((p) => ({
          id: p.id,
          name: p.name,
          color: p.color,
          isHost: p.isHost,
        }));

        ws.send(
          JSON.stringify({
            type: 'room_joined',
            roomCode: normalizedCode,
            playerId,
            playerColor: availableColor,
            isHost,
            playerCount: room.playerCount,
            players: playersList,
            gameState: room.gameState,
          })
        );

        // Notify other peers in room
        room.players.forEach((p) => {
          if (p.id !== playerId && p.ws.readyState === WebSocket.OPEN) {
            p.ws.send(
              JSON.stringify({
                type: 'player_joined',
                player: {
                  id: newPlayer.id,
                  name: newPlayer.name,
                  color: newPlayer.color,
                  isHost: newPlayer.isHost,
                },
                players: playersList,
              })
            );
          }
        });
      } else if (type === 'webrtc_signal') {
        // Forward WebRTC signal (offer, answer, ice-candidate) to target player
        const { targetPlayerId, signal } = data;
        if (!currentRoomCode) return;
        const room = rooms.get(currentRoomCode);
        if (!room) return;

        const targetPlayer = room.players.get(targetPlayerId);
        if (targetPlayer && targetPlayer.ws.readyState === WebSocket.OPEN) {
          targetPlayer.ws.send(
            JSON.stringify({
              type: 'webrtc_signal',
              fromPlayerId: currentPlayerId,
              signal,
            })
          );
        }
      } else if (type === 'game_action') {
        // Broadcast game actions (dice roll, token move, voice phrase, rematch, etc.)
        if (!currentRoomCode) return;
        const room = rooms.get(currentRoomCode);
        if (!room) return;

        const { action } = data;
        room.players.forEach((p) => {
          if (p.id !== currentPlayerId && p.ws.readyState === WebSocket.OPEN) {
            p.ws.send(
              JSON.stringify({
                type: 'game_event',
                fromPlayerId: currentPlayerId,
                action,
              })
            );
          }
        });
      } else if (type === 'sync_game_state') {
        // Host state snapshot sync
        if (!currentRoomCode) return;
        const room = rooms.get(currentRoomCode);
        if (!room) return;
        room.gameState = data.gameState;
      }
    } catch (err) {
      console.error('WS error parsing message:', err);
    }
  });

  const handleDisconnect = () => {
    if (currentRoomCode && currentPlayerId) {
      const room = rooms.get(currentRoomCode);
      if (room) {
        room.players.delete(currentPlayerId);

        if (room.players.size === 0) {
          rooms.delete(currentRoomCode);
        } else {
          // If host left, elect new host
          const remainingPlayers = Array.from(room.players.values());
          if (!remainingPlayers.some((p) => p.isHost)) {
            remainingPlayers[0].isHost = true;
          }

          const playersList = remainingPlayers.map((p) => ({
            id: p.id,
            name: p.name,
            color: p.color,
            isHost: p.isHost,
          }));

          remainingPlayers.forEach((p) => {
            if (p.ws.readyState === WebSocket.OPEN) {
              p.ws.send(
                JSON.stringify({
                  type: 'player_left',
                  playerId: currentPlayerId,
                  players: playersList,
                })
              );
            }
          });
        }
      }
    }
  };

  ws.on('close', handleDisconnect);
  ws.on('error', handleDisconnect);
});

// Setup Express and Vite middleware
async function startServer() {
  app.use(express.json());

  app.use((_req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('X-Robots-Tag', 'all');
    next();
  });

  // API health check
  app.get('/api/health', (_req, res) => {
    res.json({ status: 'ok', activeRooms: rooms.size, timestamp: Date.now() });
  });

  // Explicit PWA Manifest & SW endpoints with proper headers for PWABuilder
  const publicPath = path.resolve(__dirname, 'public');
  const getAssetPath = (filename: string) => {
    if (isProduction && fs.existsSync(path.resolve(distPath, filename))) {
      return path.resolve(distPath, filename);
    }
    return path.resolve(publicPath, filename);
  };

  const serveManifest = (_req: express.Request, res: express.Response) => {
    const manifestFile = getAssetPath('manifest.json');
    if (fs.existsSync(manifestFile)) {
      res.setHeader('Content-Type', 'application/manifest+json; charset=utf-8');
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Cache-Control', 'public, max-age=3600');
      res.sendFile(manifestFile);
    } else {
      res.status(404).end();
    }
  };

  app.get(['/app-debug.apk', '/APK_DOWNLOAD/app-debug.apk'], (_req, res) => {
    const apkFile = path.resolve(__dirname, 'APK_DOWNLOAD', 'app-debug.apk');
    if (fs.existsSync(apkFile)) {
      res.setHeader('Content-Type', 'application/vnd.android.package-archive');
      res.setHeader('Content-Disposition', 'attachment; filename="LudoFun-debug.apk"');
      res.sendFile(apkFile);
    } else {
      res.status(404).send('APK not found');
    }
  });

  app.get('/manifest.json', serveManifest);
  app.get('/manifest.webmanifest', serveManifest);

  app.get('/sw.js', (_req, res) => {
    const swFile = getAssetPath('sw.js');
    if (fs.existsSync(swFile)) {
      res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
      res.setHeader('Service-Worker-Allowed', '/');
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Cache-Control', 'no-cache');
      res.sendFile(swFile);
    } else {
      res.status(404).end();
    }
  });

  if (!isProduction) {
    const { createServer: createViteServer } = await import('vite');
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    app.use(express.static(distPath));
    app.use((req, res, next) => {
      if (req.method === 'GET' && !req.path.startsWith('/api')) {
        res.sendFile(path.resolve(distPath, 'index.html'));
      } else {
        next();
      }
    });
  }

  httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`Ludo server running on http://0.0.0.0:${PORT} (Production: ${isProduction})`);
  });
}

startServer();
