const amqp = require('amqplib');
const { MongoClient } = require('mongodb');
require('dotenv').config();

class EventConsumer {
  constructor() {
    this.rabbitConnection = null;
    this.channel = null;
    this.mongoClient = null;
    this.db = null;
    this.isRunning = false;
  }

  async connect() {
    try {
      // Conectar a RabbitMQ
      console.log('🔌 Conectando a RabbitMQ...');
      this.rabbitConnection = await amqp.connect('amqp://admin:admin123@localhost:5672/ecommerce');
      this.channel = await this.rabbitConnection.createChannel();
      
      // Configurar prefetch para control de flujo
      await this.channel.prefetch(10);
      
      // Conectar a MongoDB
      console.log('🔌 Conectando a MongoDB...');
      this.mongoClient = new MongoClient('mongodb://admin:admin123@localhost:27017', {
        maxPoolSize: 10,
        serverSelectionTimeoutMS: 5000
      });
      
      await this.mongoClient.connect();
      this.db = this.mongoClient.db('ecommerce_analytics');
      
      // Crear índices para optimizar consultas
      await this.createIndexes();
      
      console.log('✅ Conectado a RabbitMQ y MongoDB');
      
    } catch (error) {
      console.error('❌ Error de conexión:', error.message);
      throw error;
    }
  }

  async createIndexes() {
    try {
      // Índices para user_events
      await this.db.collection('user_events').createIndex({ timestamp: -1 });
      await this.db.collection('user_events').createIndex({ user_id: 1 });
      await this.db.collection('user_events').createIndex({ event_type: 1 });
      await this.db.collection('user_events').createIndex({ 'data.product_id': 1 });
      await this.db.collection('user_events').createIndex({ 'data.category': 1 });
      
      // Índices para métricas en tiempo real
      await this.db.collection('real_time_metrics').createIndex({ timestamp: -1 });
      
      // Índices para estadísticas de productos
      await this.db.collection('product_stats').createIndex({ product_id: 1, date: 1 }, { unique: true });
      
      console.log('✅ Índices creados correctamente');
    } catch (error) {
      console.warn('⚠️ Error creando índices:', error.message);
    }
  }

  async processUserEvent(event) {
    try {
      const timestamp = new Date(event.timestamp);
      
      // Validar estructura del evento
      if (!event.user_id || !event.event_type || !event.data) {
        throw new Error('Estructura de evento inválida');
      }
      
      // 1. Guardar evento raw
      await this.db.collection('user_events').insertOne({
        ...event,
        processed_at: new Date()
      });
      
      // 2. Actualizar métricas en tiempo real
      await this.updateRealTimeMetrics(event, timestamp);
      
      // 3. Procesar según tipo de evento
      switch (event.event_type) {
        case 'product_view':
          await this.updateProductViews(event, timestamp);
          break;
        case 'purchase':
          await this.processPurchase(event, timestamp);
          break;
        case 'add_to_cart':
          await this.processAddToCart(event, timestamp);
          break;
      }
      
      console.log(`✅ Evento procesado: ${event.event_type} - Usuario: ${event.user_id}`);
      
    } catch (error) {
      console.error('❌ Error procesando evento:', error.message);
      // Aquí podrías implementar una cola de dead letters
      throw error;
    }
  }

  async updateRealTimeMetrics(event, timestamp) {
    const minuteWindow = new Date(
      timestamp.getFullYear(), 
      timestamp.getMonth(), 
      timestamp.getDate(), 
      timestamp.getHours(), 
      timestamp.getMinutes(), 
      0
    );
    
    const update = {
      $inc: {
        'metrics.total_events': 1
      },
      $addToSet: {
        'unique_users': event.user_id
      },
      $set: {
        'timestamp': minuteWindow,
        'updated_at': new Date()
      }
    };

    // Incrementar contadores específicos por tipo de evento
    update.$inc[`metrics.${event.event_type}s`] = 1;

    // Calcular revenue para compras
    if (event.event_type === 'purchase') {
      const revenue = (event.data.price * event.data.quantity) * (1 - (event.data.discount || 0));
      update.$inc['metrics.revenue'] = revenue;
    }

    await this.db.collection('real_time_metrics').updateOne(
      { timestamp: minuteWindow },
      update,
      { upsert: true }
    );
  }

  async updateProductViews(event, timestamp) {
    const date = timestamp.toISOString().split('T')[0];
    
    await this.db.collection('product_stats').updateOne(
      { 
        product_id: event.data.product_id,
        date: date
      },
      {
        $inc: { 
          views: 1 
        },
        $set: { 
          product_name: event.data.product_name,
          category: event.data.category,
          brand: event.data.brand,
          price: event.data.price,
          last_viewed: timestamp
        }
      },
      { upsert: true }
    );
  }

  async processAddToCart(event, timestamp) {
    // Actualizar estadísticas de carrito
    await this.db.collection('cart_stats').updateOne(
      { 
        user_id: event.user_id,
        date: timestamp.toISOString().split('T')[0]
      },
      {
        $inc: { 
          items_added: event.data.quantity
        },
        $set: {
          last_activity: timestamp
        }
      },
      { upsert: true }
    );
  }

  async processPurchase(event, timestamp) {
    const revenue = (event.data.price * event.data.quantity) * (1 - (event.data.discount || 0));
    
    // Registrar venta individual
    await this.db.collection('sales').insertOne({
      user_id: event.user_id,
      session_id: event.session_id,
      product_id: event.data.product_id,
      product_name: event.data.product_name,
      category: event.data.category,
      brand: event.data.brand,
      quantity: event.data.quantity,
      unit_price: event.data.price,
      discount: event.data.discount || 0,
      total_amount: revenue,
      timestamp: timestamp,
      metadata: event.metadata
    });
    
    // Actualizar estadísticas diarias de productos
    const date = timestamp.toISOString().split('T')[0];
    await this.db.collection('product_stats').updateOne(
      { 
        product_id: event.data.product_id,
        date: date
      },
      {
        $inc: { 
          purchases: event.data.quantity,
          revenue: revenue
        },
        $set: {
          last_purchased: timestamp
        }
      },
      { upsert: true }
    );
  }

  async startConsuming() {
    if (!this.channel || !this.db) {
      throw new Error('Conexiones no establecidas. Llama a connect() primero.');
    }

    this.isRunning = true;
    console.log('🔄 Iniciando consumer de eventos...');
    
    await this.channel.consume('user.events', async (msg) => {
      if (!msg || !this.isRunning) return;
      
      try {
        const event = JSON.parse(msg.content.toString());
        await this.processUserEvent(event);
        this.channel.ack(msg);
        
      } catch (error) {
        console.error('❌ Error procesando mensaje:', error.message);
        // Rechazar mensaje y no reenviar (podríamos enviarlo a una dead letter queue)
        this.channel.nack(msg, false, false);
      }
    }, {
      noAck: false // Requiere acknowledgment manual
    });

    console.log('👂 Escuchando eventos en la queue: user.events');
  }

  async getStats() {
    if (!this.db) return null;
    
    try {
      const totalEvents = await this.db.collection('user_events').countDocuments();
      const totalSales = await this.db.collection('sales').countDocuments();
      const totalRevenue = await this.db.collection('sales').aggregate([
        { $group: { _id: null, total: { $sum: '$total_amount' } } }
      ]).toArray();
      
      return {
        total_events: totalEvents,
        total_sales: totalSales,
        total_revenue: totalRevenue[0]?.total || 0,
        timestamp: new Date()
      };
    } catch (error) {
      console.error('❌ Error obteniendo estadísticas:', error.message);
      return null;
    }
  }

  async close() {
    this.isRunning = false;
    
    if (this.channel) {
      await this.channel.close();
      console.log('✅ Canal RabbitMQ cerrado');
    }
    
    if (this.rabbitConnection) {
      await this.rabbitConnection.close();
      console.log('✅ Conexión RabbitMQ cerrada');
    }
    
    if (this.mongoClient) {
      await this.mongoClient.close();
      console.log('✅ Conexión MongoDB cerrada');
    }
  }
}

// Función principal
async function main() {
  const consumer = new EventConsumer();
  
  try {
    await consumer.connect();
    await consumer.startConsuming();
    
    // Mostrar estadísticas cada 60 segundos
    setInterval(async () => {
      const stats = await consumer.getStats();
      if (stats) {
        console.log('📊 Estadísticas actuales:', stats);
      }
    }, 60000);
    
  } catch (error) {
    console.error('❌ Error en la aplicación:', error);
    process.exit(1);
  }
  
  // Manejo de señales para cierre limpio
  process.on('SIGINT', async () => {
    console.log('\n🛑 Cerrando consumer...');
    await consumer.close();
    process.exit(0);
  });
}

// Ejecutar si es el archivo principal
if (require.main === module) {
  main();
}

module.exports = EventConsumer;