import express from 'express';
import path from 'path';
import { router as uploadsRouter } from './routes/uploads.js';

const app = express();
const PORT = parseInt(process.env.PORT || '8080', 10);

app.use(express.json());

// API routes
app.use('/api/uploads', uploadsRouter);

// Health check
app.get('/health', (_req, res) => {
  res.send('ok');
});

// Serve static SPA files
const staticDir = path.join(import.meta.dirname, '..', 'dist');
app.use(express.static(staticDir, {
  maxAge: '1y',
  immutable: true,
  index: false,
}));

// SPA fallback — serve index.html with no-cache for all non-API, non-static routes
app.get('*', (_req, res) => {
  res.set('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.sendFile(path.join(staticDir, 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
