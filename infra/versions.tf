terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

# Credenciales: las toma del perfil de AWS CLI (~/.aws/credentials) que copias desde
# AWS Academy > AWS Details. Nunca se escriben en este repositorio.
provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project = var.project
    }
  }
}
