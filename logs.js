const amqp = require('amqplib');
const mysql = require('mysql2/promise');

// Configuración RabbitMQ
const RABBITMQ_CONFIG = {
    protocol: "amqp",
    hostname: "158.69.131.226",
    port: 5672,
    username: "lightdata",
    password: "QQyfVBKRbw6fBb",
    heartbeat: 30,
};

// Configuración MySQL
const MYSQL_CONFIG = {
    host: "149.56.182.49",
    port: 44353,
    user: "root",
    password: "4AVtLery67GFEd",
    database: "logs", // Cambia esto si la base tiene otro nombre
};

// Conexión MySQL
let mysqlConnection;

const initMySQL = async () => {
    try {
        mysqlConnection = await mysql.createConnection(MYSQL_CONFIG);
        console.log("✅ Conectado a MySQL");
    } catch (err) {
        console.error("❌ Error conectando a MySQL:", err.message);
        process.exit(1);
    }
};

const insertLog = async (sellerId, messageObject) => {
    try {
        const dataString = JSON.stringify(messageObject);
        await mysqlConnection.execute(
            "INSERT INTO logs_callback (seller_id, data) VALUES (?, ?)",
            [sellerId, dataString]
        );
        console.log(`📦 Log guardado para seller ${sellerId}`);
    } catch (err) {
        console.error("❌ Error al guardar log en MySQL:", err.message);
    }
};

const startConsumer = async () => {
    try {
        const connection = await amqp.connect(RABBITMQ_CONFIG);
        const channel = await connection.createChannel();
        const queue = "callback_logs";

        await channel.assertQueue(queue, { durable: true });
        console.log(`🎧 Escuchando la cola "${queue}"...`);

        channel.consume(queue, async (msg) => {
            if (msg !== null) {
                try {
                    const content = msg.content.toString();
                    const logObject = JSON.parse(content);
                    const sellerId = logObject.sellerid;

                    await insertLog(sellerId, logObject);
                    channel.ack(msg);
                } catch (err) {
                    console.error("⚠️ Error procesando mensaje:", err.message);
                    channel.nack(msg, false, false); // descarta el mensaje
                }
            }
        });
    } catch (err) {
        console.error("🚨 Error al conectar a RabbitMQ:", err.message);
    }
};

// Inicializar
(async () => {
    await initMySQL();
    await startConsumer();
})();
