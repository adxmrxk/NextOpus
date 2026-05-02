################################################################################
# NextOpus - Security Module Variables
################################################################################

variable "compartment_ocid" {
  description = "OCID of the compartment"
  type        = string
}

variable "vcn_id" {
  description = "OCID of the VCN"
  type        = string
}

variable "vcn_cidr" {
  description = "CIDR block of the VCN"
  type        = string
}

variable "project_name" {
  description = "Name prefix for resources"
  type        = string
}

variable "allowed_ssh_cidr" {
  description = "CIDR block allowed to SSH (0.0.0.0/0 for anywhere, or your IP)"
  type        = string
  default     = "0.0.0.0/0"
}

variable "freeform_tags" {
  description = "Freeform tags for resources"
  type        = map(string)
  default     = {}
}
