const express = require('express'); const app = express();
app.use(express.json());
const TOKEN = process.env.INTERNAL_TOKEN || 'secret';
app.get('/health', (_req, res) => res.status(200).send('OK'));
app.post('/internal/broadcast', (req, res) => {
  if (req.get('authorization') !== `Bearer ${TOKEN}`) return res.sendStatus(401);
  return res.sendStatus(204);
});
app.listen(process.env.PORT || 8080, () => console.log('internal API on :8080'));
