# 🏦 BancoP2P — Plataforma de Transacciones Bancarias con Ledger de Eventos

> Proyecto Final — Sistemas Distribuidos y Sistemas Operativos  
> Universidad — Mayo 2026

---

## 📋 Descripción General

Este proyecto implementa el núcleo de un sistema bancario moderno **P2P (Peer-to-Peer)** usando una arquitectura de microservicios basada en eventos. Simula lo que ocurre en un banco real cuando un usuario realiza una transferencia: validación, registro permanente en un ledger inmutable, detección automática de fraude, notificaciones por email y actualización del dashboard en tiempo real.

El sistema fue construido siguiendo patrones de arquitectura distribuida utilizados en la industria financiera real: **CQRS**, **Event Sourcing** y **Fan-out**, con dos tecnologías de mensajería complementarias: **RabbitMQ** como bus de comandos y **Apache Kafka** como ledger de eventos inmutable.

Todo el sistema corre en una instancia **Ubuntu en AWS EC2**, orquestado con **Docker Compose**, con 10 servicios corriendo en paralelo y comunicándose en tiempo real.

---

## 🎯 Objetivos Cumplidos

| Objetivo | Implementación |
|----------|---------------|
| Diseño Híbrido CQRS/Event Sourcing | RabbitMQ como Command Bus + Kafka como Event Bus |
| Work Queue exactamente una vez | `channel.ack()` en transaction-processor |
| Kafka como log inmutable | `tx_id` como key, topic `transactions_log` |
| Consumidor stateful de fraude | Map en memoria + lógica `amount > 10000` |
| Fan-out a múltiples consumidores | fraud-detector + dashboard + notification-router |
| Puente Kafka → RabbitMQ | notification-router |
| Worker genérico de email | email-worker sin lógica de negocio |
| Ad generator independiente | `setInterval` cada 30 segundos |
| Dashboard WebSocket tiempo real | dashboard-aggregator + dos versiones de dashboard |
| Docker Compose ~10 servicios | 10 servicios orquestados |

---

## 🏗️ Arquitectura del Sistema

```
[Cliente HTTP]
      │
      ▼ POST /transfer
[transaction-api] ──────► RabbitMQ (transfer_commands)
                                    │
                                    ▼
                        [transaction-processor]
                                    │
                                    ▼ Publica evento
                          Kafka (transactions_log)
                         /          |              \
                        ▼           ▼               ▼
              [fraud-detector] [dashboard-   [notification-
                    │          aggregator]      router]
                    │               │               │
                    ▼               ▼               ▼
           Kafka (fraud_alerts) WebSocket    RabbitMQ (email_queue)
                    │           navegador          │
                    ▼                              ▼
           [dashboard-aggregator]           [email-worker]
           marca tx en ROJO                 simula envío
                                                   ▲
                                                   │
                                        [ad-generator]
                                        (cada 30 seg)
```

---

## 🛠️ Tecnologías Utilizadas

### Apache Kafka
Log de eventos distribuido e inmutable. Actúa como el **ledger central** del sistema. Los mensajes publicados en Kafka se conservan permanentemente aunque sean leídos. Múltiples servicios pueden consumir el mismo evento de forma independiente y simultánea. Usado para los topics `transactions_log` y `fraud_alerts`.

### RabbitMQ
Message broker orientado a comandos y tareas. Los mensajes se eliminan una vez procesados. Garantiza que cada tarea sea ejecutada exactamente una vez. Usado para las colas `transfer_commands` y `email_queue`.

### Node.js
Lenguaje de los 7 microservicios personalizados. Ideal para sistemas event-driven por su modelo asíncrono no bloqueante.

### Docker & Docker Compose
Docker empaqueta cada microservicio en un contenedor aislado. Docker Compose orquesta los 10 servicios con un solo comando, definiendo dependencias, healthchecks y orden de arranque.

### WebSocket
Comunicación bidireccional en tiempo real entre el dashboard-aggregator y el navegador del cliente.

### AWS EC2
Instancia t3.small con Ubuntu 22.04 en la nube de Amazon donde corre todo el sistema.

---

## 📦 Microservicios

### 1. `transaction-api`
**Rol:** Productor RabbitMQ  
**Puerto:** 3000  
La puerta de entrada del sistema. Expone el endpoint `POST /transfer` que recibe las solicitudes de transferencia, genera un `tx_id` único (UUID v4) y publica el comando en la cola `transfer_commands` de RabbitMQ con `persistent: true` para garantizar que no se pierda.

### 2. `transaction-processor`
**Rol:** Consumidor RabbitMQ → Productor Kafka  
El cerebro del sistema. Consume comandos de RabbitMQ, simula la validación bancaria (80% COMPLETED, 20% FAILED) y publica el evento resultante en el topic `transactions_log` de Kafka usando el `tx_id` como key para garantizar orden por transacción. Llama a `channel.ack()` para confirmar el procesamiento.

### 3. `fraud-detector`
**Rol:** Consumidor Kafka → Productor Kafka (stateful)  
Servicio stateful que lee cada transacción del topic `transactions_log`. Si el monto supera los **$10,000** la considera sospechosa y publica una alerta en el topic `fraud_alerts`. No modifica el evento original, generando un nuevo evento para mantener la inmutabilidad del ledger.

### 4. `dashboard-aggregator`
**Rol:** Consumidor Kafka → Servidor WebSocket  
Mantiene un `Map` en memoria con el estado de todas las transacciones. Consume de `transactions_log` y `fraud_alerts` (con dos consumer groups independientes) y transmite los eventos al navegador en tiempo real mediante WebSocket en el puerto 8081.

### 5. `notification-router`
**Rol:** Consumidor Kafka → Productor RabbitMQ (puente)  
El puente entre Kafka y RabbitMQ. Lee transacciones COMPLETED del topic `transactions_log` y genera tareas de email que publica en la cola `email_queue` de RabbitMQ.

### 6. `ad-generator`
**Rol:** Productor RabbitMQ independiente  
Servicio completamente desacoplado del flujo de transacciones. Cada 30 segundos publica un anuncio publicitario aleatorio en la cola `email_queue`, demostrando que el email-worker procesa tareas de múltiples fuentes sin distinguirlas.

### 7. `email-worker`
**Rol:** Consumidor RabbitMQ  
Worker genérico que consume de `email_queue` sin saber si el mensaje es una notificación de transacción o un anuncio. Implementa el principio de responsabilidad única: solo sabe "enviar emails" (simulado en consola).

---

## 🔄 Flujos de Datos

### Flujo 1: Transacción Normal ($500)
```
1. POST /transfer → transaction-api genera tx_id
2. Publica en RabbitMQ (transfer_commands)
3. transaction-processor valida → COMPLETED
4. Publica en Kafka (transactions_log)
5. En paralelo:
   ├── dashboard-aggregator → dashboard actualizado en tiempo real
   ├── fraud-detector → $500 < $10,000, ignorado
   └── notification-router → tarea de email → RabbitMQ
6. email-worker → simula envío del email
```

### Flujo 2: Transacción Sospechosa ($50,000)
```
1-4. Igual que el flujo normal
5. fraud-detector detecta $50,000 > $10,000
6. Publica alerta en Kafka (fraud_alerts)
7. dashboard-aggregator recibe la alerta
8. Marca la transacción en ROJO en el dashboard
```

---

## 🖥️ Dashboards

Este proyecto cuenta con **dos versiones del dashboard**, desarrolladas por diferentes miembros del equipo:

### Dashboard v1 — `dashboard.html`
Dashboard en tiempo real desarrollado con HTML/CSS/JS puro. Muestra las transacciones con filtros por estado, buscador de usuarios y resaltado automático en rojo de las transacciones marcadas como fraude. Se conecta al WebSocket del `dashboard-aggregator` en el puerto 8081.

Compatible directamente con el `dashboard-aggregator` del proyecto. Para usarlo, abrir el archivo en el navegador con el sistema corriendo en AWS.

### Dashboard v2 — `dashboard/index.html` (LedgerOS)
Dashboard avanzado desarrollado por el integrante Nicolás García. Incluye:
- Gráficas en tiempo real con **Chart.js** (línea de TPS, histograma de montos, donut de estados)
- Panel de control integrado para enviar transferencias directamente desde el dashboard
- **Stress Test** con envío masivo de 10, 50, 200 o 500 transacciones en ráfaga
- Monitor de estado de servicios (WebSocket, API, colas RabbitMQ)
- Event Log del sistema en tiempo real
- Diseño oscuro profesional con tipografía Syne + DM Mono

> **Nota para el evaluador:** El Dashboard v2 (LedgerOS) fue desplegado en una instancia AWS separada por el integrante Nicolás García (`http://23.22.98.233/`) con su propio backend. El protocolo WebSocket y los tipos de mensajes (`SNAPSHOT`, `TX_UPDATE`, `FRAUD_ALERT`) difieren de los del `dashboard-aggregator` de este repositorio, por lo que requiere el backend específico de esa rama para funcionar completamente. El Dashboard v1 (`dashboard.html`) es el que está integrado y funcional con el sistema principal.

---

## 🏛️ Patrones de Arquitectura

### CQRS (Command Query Responsibility Segregation)
Separación total entre comandos (escritura via RabbitMQ) y eventos (lectura via Kafka). `transaction-api` solo recibe comandos, los servicios consumidores de Kafka solo leen eventos.

### Event Sourcing
Cada transacción queda registrada permanentemente en Kafka como un evento inmutable. El `tx_id` como key garantiza el orden cronológico por transacción en el mismo partition.

### Fan-out
Un solo evento en `transactions_log` es consumido en paralelo por `fraud-detector`, `dashboard-aggregator` y `notification-router`, cada uno con su propio `groupId` y sin conocerse entre sí.

### Work Queue
RabbitMQ distribuye el trabajo entre workers. El `channel.ack()` garantiza procesamiento exactamente una vez. Si el worker falla antes del ack, RabbitMQ reencola automáticamente.

---

## 📁 Estructura del Proyecto

```
banco-p2p/
├── docker-compose.yml          # Orquestación de los 10 servicios
├── dashboard.html              # Dashboard v1 (compatible con el sistema)
├── dashboard/
│   └── index.html              # Dashboard v2 LedgerOS (rama aws)
├── transaction-api/
│   ├── Dockerfile
│   ├── package.json
│   └── index.js
├── transaction-processor/
│   ├── Dockerfile
│   ├── package.json
│   └── index.js
├── fraud-detector/
│   ├── Dockerfile
│   ├── package.json
│   └── index.js
├── dashboard-aggregator/
│   ├── Dockerfile
│   ├── package.json
│   └── index.js
├── notification-router/
│   ├── Dockerfile
│   ├── package.json
│   └── index.js
├── ad-generator/
│   ├── Dockerfile
│   ├── package.json
│   └── index.js
└── email-worker/
    ├── Dockerfile
    ├── package.json
    └── index.js
```

---

## 🚀 Instrucciones de Despliegue

### Prerrequisitos
- Docker y Docker Compose instalados
- Puerto 3000, 8081, 9092, 15672 abiertos en el firewall

### Arrancar el sistema
```bash
git clone https://github.com/dramirezdlp99/ejercicio-banco-p2p.git
cd ejercicio-banco-p2p
docker compose up --build
```

Esperar a que aparezcan los mensajes:
```
✅ Conectado a RabbitMQ
✅ Conectado a Kafka
🚀 transaction-api corriendo en puerto 3000
```

### Enviar una transacción de prueba
```bash
curl -X POST http://localhost:3000/transfer \
  -H "Content-Type: application/json" \
  -d '{"from_user": "Alice", "to_user": "Bob", "amount": 500}'
```

### Enviar una transacción sospechosa
```bash
curl -X POST http://localhost:3000/transfer \
  -H "Content-Type: application/json" \
  -d '{"from_user": "Alice", "to_user": "Bob", "amount": 50000}'
```

### Ver el dashboard
Abrir `dashboard.html` en el navegador con el sistema corriendo.

### Panel de administración RabbitMQ
```
http://localhost:15672
Usuario: guest
Contraseña: guest
```

---

## 🔌 Puertos del Sistema

| Puerto | Servicio | Descripción |
|--------|----------|-------------|
| 3000 | transaction-api | Endpoint HTTP para transferencias |
| 8081 | dashboard-aggregator | WebSocket tiempo real |
| 9092 | Kafka | Broker de eventos |
| 5672 | RabbitMQ | Protocolo AMQP |
| 15672 | RabbitMQ Management | Panel web de administración |

---

## ☁️ Infraestructura AWS

- **Instancia:** t3.small (2 vCPU, 2GB RAM)
- **OS:** Ubuntu 22.04 LTS
- **Almacenamiento:** 20GB SSD
- **Región:** us-east-2 (Ohio)
- **IP Pública:** 18.191.228.35

---

## 👥 Equipo de Desarrollo

| Integrante | Contribución principal |
|------------|----------------------|
| **Camila Bastidas** | Mejoras de frontend, Dashboard v1 mejorado, integración y pruebas |
| **Nicolás Alejandro García Pasmiño** | Dashboard v2 LedgerOS, despliegue en AWS, CI/CD |
| **David Fernando Ramírez de la Parra** | Arquitectura del sistema, microservicios backend, infraestructura AWS, orquestación Docker |