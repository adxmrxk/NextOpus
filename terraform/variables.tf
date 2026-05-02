################################################################################
# NextOpus - Root Variables
# OCI Authentication & Infrastructure Configuration
################################################################################

# ==============================================================================
# OCI Authentication Variables
# ==============================================================================

variable "tenancy_ocid" {
  description = "OCID of your OCI tenancy"
  type        = string
  sensitive   = true
}

variable "user_ocid" {
  description = "OCID of the OCI user"
  type        = string
  sensitive   = true
}

variable "fingerprint" {
  description = "Fingerprint of the OCI API signing key"
  type        = string
  sensitive   = true
}

variable "private_key_path" {
  description = "Path to the OCI API private key file"
  type        = string
}

variable "region" {
  description = "OCI region identifier"
  type        = string
  default     = "us-ashburn-1"

  validation {
    condition = contains([
      "us-ashburn-1", "us-phoenix-1", "us-sanjose-1",
      "eu-frankfurt-1", "eu-amsterdam-1", "uk-london-1",
      "ap-tokyo-1", "ap-osaka-1", "ap-seoul-1", "ap-mumbai-1",
      "sa-saopaulo-1", "ca-toronto-1", "ap-sydney-1"
    ], var.region)
    error_message = "Region must be a valid OCI region identifier."
  }
}

variable "compartment_ocid" {
  description = "OCID of the compartment (defaults to tenancy root)"
  type        = string
  default     = ""
}

# ==============================================================================
# Project Configuration
# ==============================================================================

variable "project_name" {
  description = "Name prefix for all resources"
  type        = string
  default     = "nextopus"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.project_name))
    error_message = "Project name must be lowercase, start with a letter, and be 3-21 characters."
  }
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

# ==============================================================================
# Network Configuration
# ==============================================================================

variable "vcn_cidr" {
  description = "CIDR block for the Virtual Cloud Network"
  type        = string
  default     = "10.0.0.0/16"

  validation {
    condition     = can(cidrhost(var.vcn_cidr, 0))
    error_message = "VCN CIDR must be a valid IPv4 CIDR block."
  }
}

variable "public_subnet_cidr" {
  description = "CIDR block for the public subnet"
  type        = string
  default     = "10.0.1.0/24"
}

variable "private_subnet_cidr" {
  description = "CIDR block for the private subnet"
  type        = string
  default     = "10.0.2.0/24"
}

# ==============================================================================
# Compute Configuration - Always Free Tier Constraints
# Total Available: 4 OCPUs, 24GB RAM across all A1 instances
# ==============================================================================

variable "ssh_public_key_path" {
  description = "Path to SSH public key for instance access"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "ssh_public_key" {
  description = "SSH public key content (overrides ssh_public_key_path if set)"
  type        = string
  default     = ""
}

variable "control_plane_config" {
  description = "Configuration for K3s control plane node"
  type = object({
    ocpus       = number
    memory_gb   = number
    boot_volume = number
  })
  default = {
    ocpus       = 1
    memory_gb   = 6
    boot_volume = 50
  }

  validation {
    condition     = var.control_plane_config.ocpus >= 1 && var.control_plane_config.memory_gb >= 4
    error_message = "Control plane requires at least 1 OCPU and 4GB RAM."
  }
}

variable "worker_config" {
  description = "Configuration for K3s worker nodes"
  type = object({
    count       = number
    ocpus       = number
    memory_gb   = number
    boot_volume = number
  })
  default = {
    count       = 3
    ocpus       = 1
    memory_gb   = 6
    boot_volume = 50
  }

  validation {
    condition     = var.worker_config.count >= 1 && var.worker_config.count <= 3
    error_message = "Worker count must be between 1 and 3 for Always Free Tier."
  }
}

variable "os_image_id" {
  description = "OCID of the OS image (Ubuntu 24.04 ARM). Leave empty for auto-lookup."
  type        = string
  default     = ""
}

# ==============================================================================
# Tags
# ==============================================================================

variable "freeform_tags" {
  description = "Freeform tags to apply to all resources"
  type        = map(string)
  default = {
    "ManagedBy" = "Terraform"
    "Project"   = "NextOpus"
  }
}

variable "defined_tags" {
  description = "Defined tags to apply to all resources"
  type        = map(string)
  default     = {}
}
