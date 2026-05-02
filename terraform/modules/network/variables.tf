################################################################################
# NextOpus - Network Module Variables
################################################################################

variable "compartment_ocid" {
  description = "OCID of the compartment"
  type        = string
}

variable "project_name" {
  description = "Name prefix for resources"
  type        = string
}

variable "vcn_cidr" {
  description = "CIDR block for the VCN"
  type        = string
}

variable "public_subnet_cidr" {
  description = "CIDR block for public subnet"
  type        = string
}

variable "private_subnet_cidr" {
  description = "CIDR block for private subnet"
  type        = string
}

variable "public_security_list_ids" {
  description = "List of security list OCIDs for public subnet"
  type        = list(string)
}

variable "private_security_list_ids" {
  description = "List of security list OCIDs for private subnet"
  type        = list(string)
}

variable "freeform_tags" {
  description = "Freeform tags for resources"
  type        = map(string)
  default     = {}
}
