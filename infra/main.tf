# ---------------------------------------------------------------------------
# Red: VPC propia con 2 subredes publicas en 2 zonas (sin NAT: cada EC2 tiene IP
# publica solo para salir a Internet a descargar imagenes; no se abre el puerto 22).
# ---------------------------------------------------------------------------
data "aws_availability_zones" "disponibles" {
  state = "available"
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

resource "aws_vpc" "lab" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  tags                 = { Name = "${var.project}-vpc" }
}

resource "aws_internet_gateway" "lab" {
  vpc_id = aws_vpc.lab.id
  tags   = { Name = "${var.project}-igw" }
}

locals {
  subredes = {
    a = { cidr = cidrsubnet(var.vpc_cidr, 8, 1), az = data.aws_availability_zones.disponibles.names[0] }
    b = { cidr = cidrsubnet(var.vpc_cidr, 8, 2), az = data.aws_availability_zones.disponibles.names[1] }
  }
  ip = { for k, v in var.nodos : k => v.ip }
}

resource "aws_subnet" "lab" {
  for_each                = local.subredes
  vpc_id                  = aws_vpc.lab.id
  cidr_block              = each.value.cidr
  availability_zone       = each.value.az
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.project}-subred-${each.key}" }
}

resource "aws_route_table" "publica" {
  vpc_id = aws_vpc.lab.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.lab.id
  }
  tags = { Name = "${var.project}-rt-publica" }
}

resource "aws_route_table_association" "lab" {
  for_each       = aws_subnet.lab
  subnet_id      = each.value.id
  route_table_id = aws_route_table.publica.id
}

# ---------------------------------------------------------------------------
# Security groups: el trafico Kafka solo dentro de la VPC; hacia Internet solo
# Grafana / Kafka UI / Prometheus y solo desde tu IP. Sin SSH: acceso por SSM.
# ---------------------------------------------------------------------------
resource "aws_security_group" "kafka" {
  name        = "${var.project}-kafka"
  description = "ZooKeeper y brokers Kafka"
  vpc_id      = aws_vpc.lab.id
  tags        = { Name = "${var.project}-kafka" }
}

resource "aws_security_group" "app" {
  name        = "${var.project}-app"
  description = "Productor y Kafka Streams"
  vpc_id      = aws_vpc.lab.id
  tags        = { Name = "${var.project}-app" }
}

resource "aws_security_group" "monitoring" {
  name        = "${var.project}-monitoring"
  description = "Prometheus, Grafana, Loki, Kafka UI"
  vpc_id      = aws_vpc.lab.id
  tags        = { Name = "${var.project}-monitoring" }
}

locals {
  reglas_internas = {
    zookeeper      = { sg = "kafka", desde = 2181, hasta = 2181, desc = "ZooKeeper" }
    kafka          = { sg = "kafka", desde = 9092, hasta = 9092, desc = "Kafka PLAINTEXT" }
    kafka_metricas = { sg = "kafka", desde = 7071, hasta = 7072, desc = "JMX exporter broker y ZooKeeper" }
    app_metricas   = { sg = "app", desde = 9406, hasta = 9408, desc = "JMX exporter producer y Streams" }
    loki           = { sg = "monitoring", desde = 3100, hasta = 3100, desc = "Loki push desde Alloy" }
  }
  reglas_publicas = {
    grafana      = { sg = "monitoring", desde = 3000, desc = "Grafana" }
    kafka_ui     = { sg = "monitoring", desde = 8080, desc = "Kafka UI" }
    prometheus   = { sg = "monitoring", desde = 9090, desc = "Prometheus" }
    api_usuarios = { sg = "app", desde = 8081, desc = "API REST de usuarios (Flask)" }
  }
  sg = {
    kafka      = aws_security_group.kafka.id
    app        = aws_security_group.app.id
    monitoring = aws_security_group.monitoring.id
  }
  sg_por_nodo = {
    "kafka-1"    = "kafka"
    "kafka-2"    = "kafka"
    "app"        = "app"
    "monitoring" = "monitoring"
  }
}

resource "aws_vpc_security_group_ingress_rule" "interna" {
  for_each          = local.reglas_internas
  security_group_id = local.sg[each.value.sg]
  cidr_ipv4         = var.vpc_cidr
  ip_protocol       = "tcp"
  from_port         = each.value.desde
  to_port           = each.value.hasta
  description       = each.value.desc
}

resource "aws_vpc_security_group_ingress_rule" "publica" {
  for_each          = local.reglas_publicas
  security_group_id = local.sg[each.value.sg]
  cidr_ipv4         = var.allowed_cidr
  ip_protocol       = "tcp"
  from_port         = each.value.desde
  to_port           = each.value.desde
  description       = "${each.value.desc} desde la IP del operador"
}

resource "aws_vpc_security_group_egress_rule" "todo" {
  for_each          = local.sg
  security_group_id = each.value
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# ---------------------------------------------------------------------------
# Bucket con la configuracion de despliegue (deploy/ y scripts/). Lo crea y
# llena ./verificar.sh desplegar con AWS CLI ANTES del apply: en Learner Lab una
# politica de la organizacion niega s3:GetBucketObjectLockConfiguration, que el
# recurso aws_s3_bucket del provider necesita leer. Cada EC2 descarga su parte
# en el bootstrap con el rol LabInstanceProfile.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Contrasenas generadas (no viven en el repo): terraform output -raw ...
# ---------------------------------------------------------------------------
resource "random_password" "grafana" {
  length  = 20
  special = false
}

resource "random_password" "kafka_ui" {
  length  = 20
  special = false
}

# ---------------------------------------------------------------------------
# Las 4 EC2
# ---------------------------------------------------------------------------
resource "aws_instance" "lab" {
  for_each               = var.nodos
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = each.value.tipo
  subnet_id              = aws_subnet.lab[each.value.subred].id
  private_ip             = each.value.ip
  vpc_security_group_ids = [local.sg[local.sg_por_nodo[each.key]]]
  iam_instance_profile   = var.instance_profile

  root_block_device {
    volume_type = "gp3"
    volume_size = var.volumen_gb
  }

  metadata_options {
    http_tokens = "required"
  }

  user_data = templatefile("${path.module}/bootstrap.sh.tftpl", {
    role              = each.key
    bucket            = var.bucket
    region            = var.region
    host_ip           = each.value.ip
    broker_id         = each.key == "kafka-2" ? 2 : 1
    kafka_1_ip        = local.ip["kafka-1"]
    kafka_2_ip        = local.ip["kafka-2"]
    app_ip            = local.ip["app"]
    monitoring_ip     = local.ip["monitoring"]
    grafana_password  = each.key == "monitoring" ? random_password.grafana.result : ""
    kafka_ui_password = each.key == "monitoring" ? random_password.kafka_ui.result : ""
  })

  tags = {
    Name = "${var.project}-${each.key}"
    Role = each.key
  }

  # El bootstrap corre solo en el primer arranque; cambiarlo no debe reemplazar ni reiniciar
  # las maquinas (para re-sincronizar la configuracion: ./verificar.sh sincronizar <rol>).
  lifecycle {
    ignore_changes = [user_data, ami]
  }

  depends_on = [aws_route_table_association.lab]
}
