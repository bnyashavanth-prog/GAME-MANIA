const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Middleware for parsing JSON requests (needed for analytics/ghost endpoints)
app.use(express.json({ limit: '10mb' })); // Increased limit for ghost data
app.use(express.static('public'));

// ---------------------------------------------------------
// DATABASE SETUP (SQLite)
// ---------------------------------------------------------
const dbPath = path.join(__dirname, 'game_data.db');
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error('Error opening database', err.message);
  } else {
    console.log('Connected to SQLite database.');
    
    // Create Tables
    db.serialize(() => {
      // Leaderboards
      db.run(`CREATE TABLE IF NOT EXISTS leaderboards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        level INTEGER,
        name TEXT,
        time REAL,
        timestamp INTEGER
      )`);

      // Ghosts (stores the best run array as JSON string)
      db.run(`CREATE TABLE IF NOT EXISTS ghosts (
        level INTEGER PRIMARY KEY,
        name TEXT,
        time REAL,
        run_data TEXT 
      )`);

      // Analytics (Death heatmaps)
      db.run(`CREATE TABLE IF NOT EXISTS analytics_deaths (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        level INTEGER,
        x REAL,
        z REAL,
        timestamp INTEGER
      )`);
    });
  }
});

// ---------------------------------------------------------
// REST API ENDPOINTS
// ---------------------------------------------------------

// Analytics: Log a death location
app.post('/api/analytics/death', (req, res) => {
  const { level, x, z } = req.body;
  if (level !== undefined && x !== undefined && z !== undefined) {
    db.run('INSERT INTO analytics_deaths (level, x, z, timestamp) VALUES (?, ?, ?, ?)', 
      [level, x, z, Date.now()], 
      (err) => {
        if (err) res.status(500).json({ error: err.message });
        else res.json({ success: true });
      });
  } else {
    res.status(400).json({ error: 'Missing parameters' });
  }
});

// Analytics: Get death locations for a level
app.get('/api/analytics/deaths/:level', (req, res) => {
  const level = req.params.level;
  db.all('SELECT x, z FROM analytics_deaths WHERE level = ?', [level], (err, rows) => {
    if (err) res.status(500).json({ error: err.message });
    else res.json(rows);
  });
});

// Ghosts: Get the ghost run for a level
app.get('/api/ghosts/:level', (req, res) => {
  const level = req.params.level;
  db.get('SELECT name, time, run_data FROM ghosts WHERE level = ?', [level], (err, row) => {
    if (err) {
      res.status(500).json({ error: err.message });
    } else if (row) {
      // Parse the JSON string back into an array before sending
      row.run_data = JSON.parse(row.run_data);
      res.json(row);
    } else {
      res.json(null); // No ghost yet
    }
  });
});

// Ghosts: Submit a ghost run (only saves if it's the fastest)
app.post('/api/ghosts', (req, res) => {
  const { level, name, time, run_data } = req.body;
  
  // Check if we need to update
  db.get('SELECT time FROM ghosts WHERE level = ?', [level], (err, row) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    
    // If no existing ghost, or new time is faster
    if (!row || time < row.time) {
      const dataStr = JSON.stringify(run_data);
      db.run('INSERT OR REPLACE INTO ghosts (level, name, time, run_data) VALUES (?, ?, ?, ?)',
        [level, name, time, dataStr],
        (err2) => {
          if (err2) res.status(500).json({ error: err2.message });
          else res.json({ success: true, updated: true });
        }
      );
    } else {
      res.json({ success: true, updated: false });
    }
  });
});

// ---------------------------------------------------------
// SOCKET.IO REAL-TIME MULTIPLAYER
// ---------------------------------------------------------

const players = {};

// Helper to fetch leaderboard from DB and broadcast
function broadcastLeaderboard(level) {
  db.all('SELECT name, time FROM leaderboards WHERE level = ? ORDER BY time ASC LIMIT 10', [level], (err, rows) => {
    if (!err) {
      io.to(`level_${level}`).emit('leaderboard_update', rows);
    }
  });
}

io.on('connection', (socket) => {
  console.log('A player connected:', socket.id);
  
  players[socket.id] = { 
    id: socket.id, 
    name: 'Anonymous', 
    level: 1, 
    x: 0, y: 0, z: 0, rotY: 0, 
    color: Math.floor(Math.random() * 16777215) 
  };

  socket.on('join_level', (data) => {
    const p = players[socket.id];
    if (p.room) {
      socket.leave(p.room);
      io.to(p.room).emit('player_left', socket.id);
    }

    p.name = data.name || 'Anonymous';
    p.level = data.level || 1;
    p.room = data.room ? `room_${data.room}` : `level_${p.level}_public`;
    
    socket.join(p.room);
    
    // Send existing players to new player
    const levelPlayers = Object.values(players).filter(pl => pl.room === p.room && pl.id !== socket.id);
    socket.emit('current_players', levelPlayers);
    
    // Tell others about new player
    socket.to(p.room).emit('player_joined', p);

    // Send leaderboard from DB
    broadcastLeaderboard(p.level);
  });

  socket.on('update_state', (data) => {
    const p = players[socket.id];
    if (p && p.room) {
      p.x = data.x;
      p.y = data.y;
      p.z = data.z;
      p.rotY = data.rotY;
      p.color = data.color || p.color;
      
      socket.to(p.room).emit('player_moved', p);
    }
  });

  socket.on('finish_race', (data) => {
    const p = players[socket.id];
    if (p) {
      // Save to SQLite
      db.run('INSERT INTO leaderboards (level, name, time, timestamp) VALUES (?, ?, ?, ?)',
        [p.level, p.name, parseFloat(data.time), Date.now()],
        (err) => {
          if (!err) {
            broadcastLeaderboard(p.level);
          }
        }
      );
    }
  });

  socket.on('disconnect', () => {
    console.log('Player disconnected:', socket.id);
    const p = players[socket.id];
    if (p && p.room) {
      io.to(p.room).emit('player_left', socket.id);
    }
    delete players[socket.id];
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
