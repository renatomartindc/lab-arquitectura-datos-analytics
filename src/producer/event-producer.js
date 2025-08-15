const amqp = require('amqplib');
const { faker } = require('@faker-js/faker');
require('dotenv').config();

class EventProducer {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.isRunning = false;
  }

  async connect() {
    try {
      console.log('🔌 Conectando a RabbitMQ...');
      
      // Configuración de conexión con retry
      const connectionString = 'amqp://admin:admin123@localhost:5672/ecommerce';
      this.connection = await amqp.connect(connectionString);
      this.channel = await this.connection.createChannel();
      
      // Configurar el exchange principal
      await this.channel.assertExchange('ecommerce.events', 'topic', { 
        durable: true 
      });
      
      // Configurar las queues
      await this.setupQueues();
      
      console.log('✅ Conectado exitosamente a RabbitMQ');
      
      // Manejar cierre de conexión
      this.connection.on('close', () => {
        console.log('❌ Conexión a RabbitMQ cerrada');
        this.isRunning = false;
      });

    } catch (error) {
      console.error('❌ Error conectando a RabbitMQ:', error.message);
      setTimeout(() => this.connect(), 5000); // Retry después de 5 segundos
    }
  }

  async setupQueues() {
    // Queue para eventos de usuario
    await this.channel.assertQueue('user.events', { 
      durable: true,
      arguments: {
        'x-message-ttl': 86400000, // TTL de 24 horas
        'x-max-length': 100000     // Máximo 100k mensajes
      }
    });
    
    // Queue para actualizaciones de productos
    await this.channel.assertQueue('product.updates', { 
      durable: true 
    });
    
    // Bind queues al exchange
    await this.channel.bindQueue('user.events', 'ecommerce.events', 'user.*');
    await this.channel.bindQueue('product.updates', 'ecommerce.events', 'product.*');
    
    console.log('✅ Queues configuradas correctamente');
  }

  generateUserEvent() {
    const eventTypes = ['page_view', 'product_view', 'add_to_cart', 'purchase'];
    const categories = ['electronics', 'clothing', 'books', 'home', 'sports'];
    const brands = ['Apple', 'Samsung', 'Nike', 'Adidas', 'Sony', 'LG'];
    
    const eventType = faker.helpers.arrayElement(eventTypes);
    const category = faker.helpers.arrayElement(categories);
    const price = faker.number.float({ min: 10, max: 1000, precision: 0.01 });
    
    return {
      user_id: `user_${faker.number.int({ min: 1000, max: 9999 })}`,
      session_id: `sess_${faker.string.alphanumeric(8)}`,
      event_type: eventType,
      timestamp: new Date().toISOString(),
      data: {
        product_id: `prod_${faker.number.int({ min: 1000, max: 9999 })}`,
        product_name: faker.commerce.productName(),
        category: category,
        brand: faker.helpers.arrayElement(brands),
        price: price,
        quantity: eventType === 'purchase' ? faker.number.int({ min: 1, max: 3 }) : 1,
        discount: eventType === 'purchase' ? faker.number.float({ min: 0, max: 0.3, precision: 0.01 }) : 0,
        page_url: `/${category}/${faker.string.alphanumeric(6)}`
      },
      metadata: {
        user_agent: faker.internet.userAgent(),
        ip_address: faker.internet.ip(),
        device_type: faker.helpers.arrayElement(['desktop', 'mobile', 'tablet']),
        geolocation: {
          country: faker.location.country(),
          city: faker.location.city(),
          coordinates: [faker.location.latitude(), faker.location.longitude()]
        }
      }
    };
  }

  async publishEvent(routingKey, event) {
    try {
      const message = Buffer.from(JSON.stringify(event));
      
      const published = await this.channel.publish(
        'ecommerce.events', 
        routingKey, 
        message, 
        {
          persistent: true,
          timestamp: Date.now(),
          messageId: faker.string.uuid(),
          headers: {
            'event_type': event.event_type,
            'user_id': event.user_id,
            'version': '1.0'
          }
        }
      );
      
      if (!published) {
        console.warn('⚠️ Mensaje no pudo ser publicado (buffer lleno)');
      }
      
      return published;
    } catch (error) {
      console.error('❌ Error publicando evento:', error.message);
      return false;
    }
  }

  async startSimulation(eventsPerSecond = 5, duration = null) {
    if (!this.channel) {
      console.error('❌ No hay conexión activa. Llama a connect() primero.');
      return;
    }

    this.isRunning = true;
    const intervalMs = 1000 / eventsPerSecond;
    let eventCount = 0;
    const startTime = Date.now();
    
    console.log(`🚀 Iniciando simulación: ${eventsPerSecond} eventos/segundo`);
    if (duration) {
      console.log(`⏰ Duración: ${duration} segundos`);
    }
    
    const interval = setInterval(async () => {
      if (!this.isRunning) {
        clearInterval(interval);
        return;
      }
      
      // Verificar duración si está establecida
      if (duration && (Date.now() - startTime) / 1000 > duration) {
        this.stopSimulation();
        clearInterval(interval);
        return;
      }
      
      try {
        const event = this.generateUserEvent();
        const routingKey = `user.${event.event_type}`;
        
        const published = await this.publishEvent(routingKey, event);
        
        if (published) {
          eventCount++;
          const logMessage = `📊 Evento ${eventCount}: ${event.event_type} - ${event.data.product_name} ($${event.data.price})`;
          console.log(logMessage);
        }
        
      } catch (error) {
        console.error('❌ Error en simulación:', error.message);
      }
    }, intervalMs);

    // Mostrar estadísticas cada 30 segundos
    const statsInterval = setInterval(() => {
      if (!this.isRunning) {
        clearInterval(statsInterval);
        return;
      }
      
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = (eventCount / elapsed).toFixed(2);
      console.log(`📈 Estadísticas: ${eventCount} eventos en ${elapsed.toFixed(0)}s (${rate} eventos/s)`);
    }, 30000);
  }

  stopSimulation() {
    this.isRunning = false;
    console.log('⏹️ Simulación detenida');
  }

  async close() {
    this.stopSimulation();
    
    if (this.channel) {
      await this.channel.close();
      console.log('✅ Canal RabbitMQ cerrado');
    }
    
    if (this.connection) {
      await this.connection.close();
      console.log('✅ Conexión RabbitMQ cerrada');
    }
  }
}

// Función principal
async function main() {
  const producer = new EventProducer();
  
  try {
    await producer.connect();
    
    // Configuración desde argumentos de línea de comandos
    const eventsPerSecond = parseInt(process.argv[2]) || 5;
    const duration = parseInt(process.argv[3]) || null;
    
    await producer.startSimulation(eventsPerSecond, duration);
    
  } catch (error) {
    console.error('❌ Error en la aplicación:', error);
  }
  
  // Manejo de señales para cierre limpio
  process.on('SIGINT', async () => {
    console.log('\n🛑 Cerrando producer...');
    await producer.close();
    process.exit(0);
  });
}

// Ejecutar si es el archivo principal
if (require.main === module) {
  main();
}

module.exports = EventProducer;