# Plan AWS + GitHub Actions (Despliegue Manual)

Fecha: 2026-05-18

## Objetivo
Dejar AWS listo y habilitar deploy manual desde GitHub Actions al nodo de servicios (EC2).

---

## Parte A — AWS (Infraestructura)

### 1) Cuenta y seguridad
- [ ] Habilitar MFA en la cuenta raiz.
- [ ] Crear presupuesto de $1 con alerta al 80%.

### 2) IAM (usuarios y roles)
- [ ] Crear grupo IAM `dev-team-ledger`.
- [ ] Adjuntar politicas: `AmazonEC2FullAccess`, `AmazonMQFullAccess`, `AmazonEC2ContainerRegistryFullAccess`, `CloudWatchFullAccess`.
- [ ] Crear usuarios IAM (ej: `iam-user-infra`, `iam-user-lead`, `iam-user-backend`).
- [ ] Generar Access Keys y guardar seguro.
- [ ] Crear role `ec2-ledger-role` con trust policy para EC2.
- [ ] Crear instance profile `ec2-ledger-profile` y asociar el role.

### 3) VPC y red
- [ ] Crear VPC `vpc-ledger` (10.0.0.0/16).
- [ ] Crear subnet publica `subnet-ledger-pub` (10.0.1.0/24, us-east-1a).
- [ ] Habilitar DNS support/hostnames.
- [ ] Crear Internet Gateway y asociarlo.
- [ ] Configurar route table con salida 0.0.0.0/0.

### 4) Security Groups
- [ ] `sg-amazonmq`: 5671 interno, 15671 abierto (consola).
- [ ] `sg-kafka`: 9092 interno.
- [ ] `sg-services`: 3000 y 8081 publico.
- [ ] `sg-ssh`: 22 restringido a IP del equipo.

### 5) Amazon MQ (RabbitMQ)
- [ ] Crear broker `ledger-rabbitmq` (mq.t3.micro).
- [ ] Guardar endpoint AMQPS.
- [ ] Crear colas `transfer_commands` y `email_queue`.
- [ ] Crear exchange `dlx.transfer_commands`.

### 6) EC2
- [ ] Crear keypair `ledger-keypair` y guardar .pem.
- [ ] Lanzar EC2 Kafka (t3.micro) con `sg-kafka` + `sg-ssh`.
- [ ] Lanzar EC2 Services (t3.micro) con `sg-services` + `sg-ssh`.
- [ ] Instalar Docker y Docker Compose v2 en ambas instancias.

#### Comandos en Ubuntu (ambas instancias)

```bash
sudo apt-get update -y
sudo apt-get install -y docker.io git curl
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker $USER

# Docker Compose v2 (plugin)
mkdir -p ~/.docker/cli-plugins
curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
  -o ~/.docker/cli-plugins/docker-compose
chmod +x ~/.docker/cli-plugins/docker-compose

# Salir y volver a entrar para aplicar grupo docker
exit
```

### 7) Kafka en EC2
- [ ] Instalar Java 21.
- [ ] Instalar Kafka 3.7 en /opt/kafka.
- [ ] Configurar KRaft y systemd.
- [ ] Crear topics `transactions_log` y `fraud_alerts`.

#### Comandos en Ubuntu (nodo Kafka)

```bash
sudo apt-get update -y
sudo apt-get install -y openjdk-21-jre-headless

# Kafka 3.7
cd /tmp
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-3.7.0.tgz
sudo mv kafka_2.13-3.7.0 /opt/kafka
sudo mkdir -p /var/lib/kafka/data
sudo chown -R $USER:$USER /opt/kafka /var/lib/kafka

# Cluster UUID
CLUSTER_UUID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
echo "Cluster UUID: $CLUSTER_UUID"

# Config KRaft
cat > /opt/kafka/config/kraft/server.properties << 'EOF'
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://<KAFKA-EC2-PRIVATE-IP>:9092

controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT

log.dirs=/var/lib/kafka/data
num.partitions=3
default.replication.factor=1
log.retention.hours=168
log.segment.bytes=1073741824
auto.create.topics.enable=false

num.network.threads=2
num.io.threads=4
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
EOF

# Formatear storage
KAFKA_HEAP_OPTS="-Xmx256m -Xms128m" \
  /opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_UUID \
  -c /opt/kafka/config/kraft/server.properties

# Servicio systemd
sudo tee /etc/systemd/system/kafka.service << 'EOF'
[Unit]
Description=Apache Kafka Server (KRaft mode)
After=network.target

[Service]
Type=simple
User=$USER
Environment="KAFKA_HEAP_OPTS=-Xmx256m -Xms128m"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable kafka
sudo systemctl start kafka

# Crear topics
/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic transactions_log \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic fraud_alerts \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=259200000

/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## Parte B — Preparar el nodo de servicios (EC2)

### 8) Clonar el repo
- [ ] `git clone` en la instancia de servicios.
- [ ] Revisar que el branch de deploy exista (ej: `aws`).

#### Comandos en Ubuntu (nodo Services)

```bash
git clone https://github.com/dramirezdlp99/ejercicio-banco-p2p.git
cd ejercicio-banco-p2p
git checkout aws
```

### 9) Variables de entorno
- [ ] Crear `.env` basado en `.env.example` con:
  - `RABBITMQ_URL` (AMQPS real)
  - `KAFKA_BROKER` (IP privada EC2 Kafka)
  - `RABBITMQ_HTTP_API` (endpoint de management)
  - `RABBITMQ_USER` y `RABBITMQ_PASS` (credenciales del broker)

#### Como obtener los datos (AWS console)

RabbitMQ (Amazon MQ):
- Entrar a Amazon MQ -> Brokers -> `ledger-rabbitmq`.
- En "Connections":
  - `AMQP+SSL` -> usar en `RABBITMQ_URL`.
  - `HTTPS` -> usar en `RABBITMQ_HTTP_API` (puerto 15671).
- `RABBITMQ_USER` y `RABBITMQ_PASS`: son las credenciales del broker que creaste.

Kafka (EC2):
- En la instancia Kafka, usar la IP privada (VPC) como `KAFKA_BROKER`, ejemplo `10.0.1.x:9092`.

### 10) Primer deploy manual
- [ ] `docker compose up -d --build`
- [ ] Verificar `docker compose ps`

#### Comandos en Ubuntu (nodo Services)

```bash
docker compose up -d --build
docker compose ps
```

---

## Parte C — GitHub Actions (Deploy Manual)

### 11) Preparar acceso SSH
- [ ] Generar una clave SSH dedicada para GitHub Actions.
- [ ] Agregar la clave publica al `~/.ssh/authorized_keys` del usuario en EC2 Services.
- [ ] Validar acceso SSH desde local.

#### Comandos en Ubuntu (nodo Services)

```bash
mkdir -p ~/.ssh
chmod 700 ~/.ssh
touch ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# Pegar la clave publica (una linea completa) y guardar
nano ~/.ssh/authorized_keys
```

#### Generar la clave local (tu maquina)

```bash
ssh-keygen -t ed25519 -C "github-actions" -f ~/.ssh/gha_ledger
cat ~/.ssh/gha_ledger.pub
```

#### Probar acceso (tu maquina)

```bash
ssh -i ~/.ssh/gha_ledger ubuntu@<EC2_PUBLIC_IP>
```

### 12) Configurar Secrets en GitHub
En el repo, Settings -> Secrets and variables -> Actions:
- [ ] `EC2_HOST` = IP publica del EC2 Services.
- [ ] `EC2_USER` = usuario (ej: `ec2-user`).
- [ ] `EC2_SSH_KEY` = clave privada (contenido completo).
- [ ] `EC2_PATH` = ruta del repo en EC2 (ej: `/home/ec2-user/ejercicio-banco-p2p`).
- [ ] `EC2_BRANCH` = `aws` (opcional; default).

### 13) Ejecutar deploy manual
- [ ] Ir a Actions -> Deploy (manual) -> Run workflow.
- [ ] Verificar que los contenedores queden en `Up`.

---

## Parte D — Validaciones finales
- [ ] Probar `POST /transfer` con monto bajo.
- [ ] Probar `POST /transfer` con monto alto.
- [ ] Probar fraude por velocidad.
- [ ] Verificar dashboard en `http://<EC2_IP>/`.

---

## Notas
- Apagar servicios cuando no se usen (free tier).
- Mantener SG SSH restringido por IP.
- Rotar claves si se comparte el repo.
