variable "region" {
  description = "Region de AWS Academy Learner Lab"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Prefijo y tag Project de todos los recursos"
  type        = string
  default     = "ep5-kafka"
}

variable "allowed_cidr" {
  description = "Tu IP publica en formato CIDR (x.x.x.x/32) para abrir Grafana, Kafka UI y Prometheus. verificar.sh la detecta sola."
  type        = string
  validation {
    condition     = can(cidrhost(var.allowed_cidr, 0))
    error_message = "allowed_cidr debe ser un CIDR valido, por ejemplo 200.1.2.3/32."
  }
}

variable "instance_profile" {
  description = "Perfil IAM existente en Learner Lab (permite SSM y S3). En Learner Lab no se pueden crear roles."
  type        = string
  default     = "LabInstanceProfile"
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

# IP privadas fijas: no cambian al apagar/encender las EC2, por eso Kafka las anuncia
# (advertised.listeners) y los clientes las usan.
variable "nodos" {
  description = "Las 4 maquinas: subred (a|b), IP privada fija y tipo de instancia"
  type = map(object({
    subred = string
    ip     = string
    tipo   = string
  }))
  default = {
    "kafka-1"    = { subred = "a", ip = "10.20.1.10", tipo = "t3.small" }
    "kafka-2"    = { subred = "b", ip = "10.20.2.10", tipo = "t3.small" }
    "app"        = { subred = "b", ip = "10.20.2.20", tipo = "t3.small" }
    "monitoring" = { subred = "b", ip = "10.20.2.30", tipo = "t3.medium" }
  }
}

variable "volumen_gb" {
  type    = number
  default = 20
}

variable "bucket" {
  description = "Bucket S3 con deploy/ y scripts/ (lo crea ./verificar.sh desplegar)"
  type        = string
}
