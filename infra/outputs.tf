output "instancias" {
  description = "Rol -> id, IP privada (fija) e IP publica (cambia al apagar/encender)"
  value = {
    for k, i in aws_instance.lab : k => {
      id         = i.id
      ip_privada = i.private_ip
      ip_publica = i.public_ip
      tipo       = i.instance_type
      zona       = i.availability_zone
    }
  }
}

output "bucket" {
  value = var.bucket
}

output "api_usuarios_url" {
  value = "http://${aws_instance.lab["app"].public_ip}:8081/api/usuarios"
}

output "grafana_url" {
  value = "http://${aws_instance.lab["monitoring"].public_ip}:3000"
}

output "kafka_ui_url" {
  value = "http://${aws_instance.lab["monitoring"].public_ip}:8080"
}

output "prometheus_url" {
  value = "http://${aws_instance.lab["monitoring"].public_ip}:9090"
}

output "grafana_password" {
  description = "Usuario admin. Ver con: terraform -chdir=infra output -raw grafana_password"
  value       = random_password.grafana.result
  sensitive   = true
}

output "kafka_ui_password" {
  description = "Usuario admin. Ver con: terraform -chdir=infra output -raw kafka_ui_password"
  value       = random_password.kafka_ui.result
  sensitive   = true
}
