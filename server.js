const express = require('express');
const multer = require('multer');
const qrcode = require('qrcode');
const fs = require('fs');
const path = require('path');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');

// BLINDAGEM CONTRA CRASH DO SERVIDOR
process.on('uncaughtException', console.error);
process.on('unhandledRejection', console.error);

const app = express();
const PORT = process.env.PORT || 3000;
const upload = multer({ storage: multer.memoryStorage() });

app.use(express.json());
app.use(express.static(path.join(__dirname)));

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
    clients.forEach(client => {
        try { client.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`); } catch(e){}
    });
}

// ---------------------------------------------------------
// CONFIGURAÇÃO WHATSAPP-WEB.JS PARA VPS
// ---------------------------------------------------------
const client = new Client({
    authStrategy: new LocalAuth({ dataPath: 'wwebjs_auth' }),
    puppeteer: {
        headless: true, // Roda sem interface gráfica (Obrigatório em VPS)
        args: [
            '--no-sandbox', // Obrigatório para VPS/Linux (root)
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage', // Evita crash por falta de memória compartilhada
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
            '--single-process', 
            '--disable-gpu'
        ]
    }
});

client.on('qr', async (qr) => {
    waQr = await qrcode.toDataURL(qr);
    waStatus = { status: 'desconectado', phone: '' };
    broadcast('qr', waQr);
    broadcast('status', waStatus);
});

client.on('ready', () => {
    waQr = ''; 
    const phone = client.info.wid.user; // Pega o número conectado
    waStatus = { status: 'conectado', phone: phone };
    broadcast('status', waStatus);
    broadcast('pairing-success', true);
    console.log(`WhatsApp Conectado: +${phone}`);
});

client.on('disconnected', async (reason) => {
    console.log('Cliente desconectado:', reason);
    waStatus = { status: 'desconectado', phone: '' };
    waQr = '';
    broadcast('status', waStatus);
    
    // Reinicia o cliente após desconexão
    try { await client.destroy(); } catch(e) {}
    client.initialize();
});

// Sistema de Opt-out (Remoção da lista)
client.on('message', async (msg) => {
    const text = msg.body.toLowerCase().trim();
    if (['parar', 'sair', 'cancelar', 'stop', 'optout'].includes(text)) {
        const numOnly = msg.from.split('@')[0];
        blocklist.add(numOnly);
        saveBlocklist();
        console.log(`Número adicionado à blocklist: ${numOnly}`);
    }
});

// Inicia o cliente
client.initialize();

// ---------------------------------------------------------
// MOTOR DA FILA DE ENVIOS
// ---------------------------------------------------------
async function processQueue() {
    if (isProcessing) return;
    isProcessing = true;

    while (queue.status === 'executando' && queue.currentIndex < queue.items.length) {
        const item = queue.items[queue.currentIndex];
        
        if (item.status === 'pendente') {
            const parts = item.number.split(',');
            const numOnly = parts[0].trim();
            const nameOnly = parts.length > 1 ? parts[1].trim() : '';
            
            // O padrão do whatsapp-web.js para números é @c.us
            const jid = numOnly + '@c.us';

            if (blocklist.has(numOnly)) {
                item.status = 'falhou'; queue.stats.falharam++; queue.stats.pendentes--;
            } else {
                try {
                    let msgText = queue.message.replace(/{{numero}}/g, numOnly).replace(/{{nome}}/g, nameOnly);
                    
                    if (queue.image) {
                        // Prepara a imagem usando o Buffer do Multer
                        const media = new MessageMedia(
                            queue.image.mimetype, 
                            queue.image.buffer.toString('base64'), 
                            queue.image.originalname
                        );
                        await client.sendMessage(jid, media, { caption: msgText });
                    } else {
                        await client.sendMessage(jid, msgText);
                    }
                    
                    item.status = 'enviado'; queue.stats.enviados++; queue.stats.pendentes--;
                } catch (error) {
                    console.error(`Erro ao enviar para ${numOnly}:`, error.message);
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
        queue.status = 'concluido'; 
        broadcast('queue-state', { status: queue.status });
    }
    
    isProcessing = false;
}

// ---------------------------------------------------------
// ROTAS / API
// ---------------------------------------------------------
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

app.post('/api/connect', (req, res) => { 
    if (waStatus.status === 'desconectado') client.initialize(); 
    res.json({ success: true }); 
});

app.post('/api/disconnect', async (req, res) => { 
    try {
        await client.logout();
        waStatus = { status: 'desconectado', phone: '' };
        broadcast('status', waStatus);
    } catch(e) { }
    res.json({ success: true }); 
});

// Endpoint de Pareamento usando o whatsapp-web.js
app.post('/api/pair', async (req, res) => {
    const phone = req.body.phone;
    
    if (waStatus.status === 'conectado') {
        return res.status(400).json({error: "O WhatsApp já está conectado neste servidor."});
    }

    try {
        // No whatsapp-web.js, se o QR Code já foi carregado, a página está pronta para receber o código.
        const code = await client.requestPairingCode(phone);
        const formattedCode = code?.match(/.{1,4}/g)?.join('-') || code;
        return res.json({ success: true, code: formattedCode });
    } catch (e) {
        console.error("Erro ao gerar o código:", e.message);
        return res.status(500).json({ error: "Aguarde o QR Code aparecer na tela ou reinicie o sistema antes de pedir o código." });
    }
});

// Endpoint de disparo
app.post('/api/send', upload.single('image'), (req, res) => {
    const numbers = JSON.parse(req.body.numbers);
    queue.items = numbers.map(num => ({ number: num, status: 'pendente' }));
    queue.message = req.body.message || ''; 
    queue.interval = parseInt(req.body.interval) || 0;
    queue.image = req.file || null; // Salva o objeto inteiro do multer (buffer, mimetype, etc)
    queue.currentIndex = 0; 
    queue.status = 'executando';
    queue.stats = { enviados: 0, falharam: 0, pendentes: numbers.length, total: numbers.length };
    
    broadcast('queue-start', queue.items); 
    broadcast('queue-stats', queue.stats); 
    broadcast('queue-state', { status: queue.status });
    
    processQueue(); 
    res.json({ success: true });
});

app.post('/api/queue/pause', (req, res) => { if(queue.status === 'executando') queue.status = 'pausado'; broadcast('queue-state', { status: queue.status }); res.json({ success: true }); });
app.post('/api/queue/resume', (req, res) => { if(queue.status === 'pausado') { queue.status = 'executando'; processQueue(); } broadcast('queue-state', { status: queue.status }); res.json({ success: true }); });
app.post('/api/queue/cancel', (req, res) => { queue.status = 'cancelado'; broadcast('queue-state', { status: queue.status }); res.json({ success: true }); });

app.listen(PORT, '0.0.0.0', () => console.log(`🚀 Sistema rodando na porta ${PORT} com whatsapp-web.js`));