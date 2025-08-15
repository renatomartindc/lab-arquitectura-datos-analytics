
## Local Development Setup
* Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
* Install [Nodejs v16+](https://nodejs.org/id/blog/release/v16.16.0/) 

## Local Installation

* **Infraestructure Installation**: 

  * Git Clone repository in folder local Example: D:\lab\analytics\ (windows) or home/lab/analytics/ (Linux)
  * Into folder local, Go to root project \
  * Execute the instruction: > docker-compose up -d
  * Verify docker containers up
  * Verificar RabbitMQ Management
     * curl http://localhost:15672
     <!--    Usuario: admin, Contraseña: admin123 -->
  * Verificar MongoDB
    * curl http://localhost:8081
     <!--   Usuario: admin, Contraseña: admin123 -->
  * Verificar conexión MongoDB directa
     * Ejecutar: >docker exec -it ecommerce-mongodb mongosh -u admin -p admin123

* **Development Installation**: 

* Go to each folder spring-boot-microservices-course\ (api-gateway, catalog-service, order-service, notification-service, bookstore-webapp) in this order.
* In each folder (Example: spring-boot-microservices-course\api-gateway) Execute the instruction: > mvn clean install
* Verify Build Success ![Build Success](docs/build.png)
* Then, execute the instruction: > mvn spring-boot:run
* Verify Run Sucess ![Run Success](docs/run.png)

* **Test BookStore-WebApp**:
* Go to page http://localhost:8080 ![BookStore Page](docs/webapp.png)

  





