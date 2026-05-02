################################################################################
# NextOpus - Compute Module Variables
################################################################################

variable "compartment_ocid" {
  description = "OCID of the compartment"
  type        = string
}

variable "project_name" {
  description = "Name prefix for resources"
  type        = string
}

variable "subnet_id" {
  description = "OCID of the subnet for instances"
  type        = string
}

variable "nsg_ids" {
  description = "List of Network Security Group OCIDs"
  type        = list(string)
  default     = []
}

variable "ssh_public_key" {
  description = "SSH public key for instance access (if empty, one will be generated)"
  type        = string
  default     = ""
}

variable "os_image_id" {
  description = "OCID of the OS image (leave empty for Ubuntu 24.04 ARM auto-lookup)"
  type        = string
  default     = ""
}

variable "control_plane_config" {
  description = "Configuration for the K3s control plane node"
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
}

variable "freeform_tags" {
  description = "Freeform tags for resources"
  type        = map(string)
  default     = {}
}
