const express = require('express');
const multer = require('multer');
const qrcode = require('qrcode');
const fs = require('fs');
const path = require('path');
const pino = require('pino');
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, Browsers } = require('@whiskeysockets/baileys');

const app = express();
const PORT = process.env.PORT || 3000;
const upload = multer({ storage: multer.memoryStorage() });

app.use(express.json());
app.use(express.static(path.join(__dirname)));

let sock = null;
let waStatus = { status: 'desconectado', phone: '' };
let waQr = '';
let clients = []; 

const BLOCKLIST_FILE = path.join(__dirname, 'blocklist.json');
let blocklist = new Set();
if (fs.existsSync(BLOCKLIST_FILE)) {
    try {
        const data = JSON.parse(fs.readFileSync(BLOCKLIST_FILE));
        blocklist = new Set(data);
    } catch (e) {}
}

function saveBlocklist() {
    fs.writeFileSync(BLOCKLIST_FILE, JSON.stringify(Array.from(blocklist)));
}

let queue = {
    items: [], message: '', image: null, interval: 10,
    currentIndex: 0, status: 'ocioso',
    stats: { enviados: 0, falharam: 0, pendentes: 0, total: 0 }
};
let isProcessing = false;

function broadcast(event, data) {
    clients.forEach(client => client.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
}

// Inicialização com limpeza garantida
async function connectToWhatsApp() {
    if (sock) {
        try { sock.ws.close(); } catch(e) {}
        try { sock.ev.removeAllListeners(); } catch(e) {}
        sock = null;
    }

    const { state, saveCreds } = await useMultiFileAuthState('baileys_auth_info');
    
    sock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        logger: pino({ level: 'silent' }),
        browser: Browsers.ubuntu('Chrome') 
    });

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;
        
        if (qr) {
            waQr = await qrcode.toDataURL(qr);
            waStatus = { status: 'desconectado', phone: '' };
            broadcast('qr', waQr);
            broadcast('status', waStatus);
        }
        
        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect.error?.output?.statusCode !== DisconnectReason.loggedOut);
            waQr = ''; 
            if (shouldReconnect) {
                setTimeout(connectToWhatsApp, 3000);
            } else {
                waStatus = { status: 'desconectado', phone: '' };
                broadcast('status', waStatus);
            }
        } else if (connection === 'open') {
            waQr = ''; 
            const phone = sock.user.id.split(':')[0];
            waStatus = { status: 'conectado', phone: phone };
            broadcast('status', waStatus);
            broadcast('pairing-success', true);
        }
    });

    sock.ev.on('creds.update', saveCreds);
    sock.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message || msg.key.fromMe) return;
        const text = msg.message.conversation || msg.message.extendedTextMessage?.text || "";
        if (['parar', 'sair', 'cancelar', 'stop', 'optout'].includes(text.toLowerCase().trim())) {
            blocklist.add(msg.key.remoteJid.split('@')[0]);
            saveBlocklist();
        }
    });
}

// Motor da Fila
async function processQueue() {
    if (isProcessing) return;
    isProcessing = true;

    while (queue.status === 'executando' && queue.currentIndex < queue.items.length) {
        const item = queue.items[queue.currentIndex];
        if (item.status === 'pendente') {
            const parts = item.number.split(',');
            const numOnly = parts[0];
            const nameOnly = parts.length > 1 ? parts[1].trim() : '';
            const jid = numOnly + '@s.whatsapp.net';

            if (blocklist.has(numOnly)) {
                item.status = 'falhou'; queue.stats.falharam++; queue.stats.pendentes--;
            } else {
                try {
                    let msgText = queue.message.replace(/{{numero}}/g, numOnly).replace(/{{nome}}/g, nameOnly);
                    if (queue.image) {
                        await sock.sendMessage(jid, { image: queue.image, caption: msgText });
                    } else {
                        await sock.sendMessage(jid, { text: msgText });
                    }
                    item.status = 'enviado'; queue.stats.enviados++; queue.stats.pendentes--;
                } catch (error) {
                    item.status = 'falhou'; queue.stats.falharam++; queue.stats.pendentes--;
                }
            }
            broadcast('queue-update', { index: queue.currentIndex, status: item.status });
            broadcast('queue-stats', queue.stats);
        }
        queue.currentIndex++;
        if (queue.currentIndex < queue.items.length && queue.status === 'executando') {
            await new Promise(resolve => setTimeout(resolve, queue.interval > 0 ? queue.interval * 1000 : 50));
        }
    }
    if (queue.currentIndex >= queue.items.length) {
        queue.status = 'concluido'; broadcast('queue-state', { status: queue.status });
    }
    isProcessing = false;
}

// Endpoints
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/api/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    clients.push(res);
    res.write(`event: status\ndata: ${JSON.stringify(waStatus)}\n\n`);
    if(waQr) res.write(`event: qr\ndata: ${JSON.stringify(waQr)}\n\n`);
    req.on('close', () => { clients = clients.filter(c => c !== res); });
});

app.post('/api/connect', (req, res) => { if (waStatus.status === 'desconectado') connectToWhatsApp(); res.json({ success: true }); });
app.post('/api/disconnect', async (req, res) => { if (sock) { await sock.logout(); } res.json({ success: true }); });

// ENDPOINT DE GERAÇÃO COM RETRY AUTOMÁTICO
app.post('/api/pair', async (req, res) => {
    try {
        const phone = req.body.phone;
        if (!sock) return res.status(400).json({error: "Sistema iniciando, aguarde alguns segundos..."});
        if (sock.authState.creds.registered) return res.status(400).json({error: "O WhatsApp já está conectado."});
        
        // Se a conexão morreu, reinicia e diz ao front para tentar sozinho novamente
        if (!sock.ws || sock.ws.readyState !== 1) {
            connectToWhatsApp();
            return res.status(200).json({ retry: true }); 
        }

        const code = await sock.requestPairingCode(phone);
        const formattedCode = code?.match(/.{1,4}/g)?.join('-') || code;
        res.json({ success: true, code: formattedCode });

    } catch(e) {
        // Erro 428 (Connection Closed) - Dispara o Auto-Retry
        if (e.message === 'Connection Closed' || e?.output?.statusCode === 428) {
            connectToWhatsApp();
            return res.status(200).json({ retry: true });
        }
        res.status(500).json({error: "Falha ao gerar código. Tente usar o QR Code."});
    }
});

app.post('/api/send', upload.single('image'), (req, res) => {
    const numbers = JSON.parse(req.body.numbers);
    queue.items = numbers.map(num => ({ number: num, status: 'pendente' }));
    queue.message = req.body.message || ''; queue.interval = parseInt(req.body.interval) || 0;
    queue.image = req.file ? req.file.buffer : null; queue.currentIndex = 0; queue.status = 'executando';
    queue.stats = { enviados: 0, falharam: 0, pendentes: numbers.length, total: numbers.length };
    broadcast('queue-start', queue.items); broadcast('queue-stats', queue.stats); broadcast('queue-state', { status: queue.status });
    processQueue(); res.json({ success: true });
});

app.post('/api/queue/pause', (req, res) => { if(queue.status === 'executando') queue.status = 'pausado'; broadcast('queue-state', { status: queue.status }); res.json({ success: true }); });
app.post('/api/queue/resume', (req, res) => { if(queue.status === 'pausado') { queue.status = 'executando'; processQueue(); } broadcast('queue-state', { status: queue.status }); res.json({ success: true }); });
app.post('/api/queue/cancel', (req, res) => { queue.status = 'cancelado'; broadcast('queue-state', { status: queue.status }); res.json({ success: true }); });

connectToWhatsApp();
app.listen(PORT, '0.0.0.0', () => console.log(`Rodando na porta ${PORT}`));