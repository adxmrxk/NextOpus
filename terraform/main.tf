################################################################################
# NextOpus - Root Module
# Orchestrates infrastructure provisioning on Oracle Cloud Always Free Tier
################################################################################

# ==============================================================================
# Locals
# ==============================================================================

locals {
  # Use tenancy as compartment if not specified
  compartment_ocid = var.compartment_ocid != "" ? var.compartment_ocid : var.tenancy_ocid

  # Merge project tags
  common_tags = merge(var.freeform_tags, {
    "Project"     = var.project_name
    "Environment" = var.environment
    "Terraform"   = "true"
  })

  # Read SSH key from file if not provided directly
  ssh_public_key = var.ssh_public_key != "" ? var.ssh_public_key : (
    fileexists(pathexpand(var.ssh_public_key_path)) ?
    trimspace(file(pathexpand(var.ssh_public_key_path))) : ""
  )
}

# ==============================================================================
# Security Module
# Must be created first as network module depends on security list IDs
# ==============================================================================

module "security" {
  source = "./modules/security"

  compartment_ocid = local.compartment_ocid
  vcn_id           = module.network.vcn_id
  vcn_cidr         = var.vcn_cidr
  project_name     = var.project_name
  allowed_ssh_cidr = "0.0.0.0/0" # Restrict in production!
  freeform_tags    = local.common_tags

  depends_on = [module.network]
}

# ==============================================================================
# Network Module
# Creates VCN, Subnets, and Gateways
# ==============================================================================

module "network" {
  source = "./modules/network"

  compartment_ocid         = local.compartment_ocid
  project_name             = var.project_name
  vcn_cidr                 = var.vcn_cidr
  public_subnet_cidr       = var.public_subnet_cidr
  private_subnet_cidr      = var.private_subnet_cidr
  public_security_list_ids = [module.security.public_security_list_id]
  private_security_list_ids = [module.security.private_security_list_id]
  freeform_tags            = local.common_tags

  # Note: This creates a circular dependency that Terraform handles
  # by creating the VCN first, then security lists, then subnets
}

# ==============================================================================
# Compute Module
# Provisions 4 ARM instances (1 Control Plane + 3 Workers)
# ==============================================================================

module "compute" {
  source = "./modules/compute"

  compartment_ocid     = local.compartment_ocid
  project_name         = var.project_name
  subnet_id            = module.network.public_subnet_id
  nsg_ids              = module.security.k3s_nsg_ids
  ssh_public_key       = local.ssh_public_key
  os_image_id          = var.os_image_id
  control_plane_config = var.control_plane_config
  worker_config        = var.worker_config
  freeform_tags        = local.common_tags

  depends_on = [
    module.network,
    module.security
  ]
}

# ==============================================================================
# Generate Ansible Inventory
# ==============================================================================

resource "local_file" "ansible_inventory" {
  filename = "${path.root}/../ansible/inventory/hosts.yml"
  content  = yamlencode({
    all = {
      vars = {
        ansible_user                 = "ubuntu"
        ansible_ssh_private_key_file = module.compute.generated_ssh_private_key_path != null ? module.compute.generated_ssh_private_key_path : "~/.ssh/id_rsa"
        ansible_python_interpreter   = "/usr/bin/python3"
        ansible_ssh_common_args      = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
      }
      children = {
        k3s_cluster = {
          children = {
            control_plane = {
              hosts = {
                (module.compute.control_plane_display_name) = {
                  ansible_host = module.compute.control_plane_public_ip
                  private_ip   = module.compute.control_plane_private_ip
                  k3s_role     = "server"
                }
              }
            }
            workers = {
              hosts = {
                for idx, name in module.compute.worker_display_names :
                name => {
                  ansible_host = module.compute.worker_public_ips[idx]
                  private_ip   = module.compute.worker_private_ips[idx]
                  k3s_role     = "agent"
                  worker_index = idx + 1
                }
              }
            }
          }
        }
      }
    }
  })

  file_permission = "0644"

  depends_on = [module.compute]
}

# Also generate INI format for compatibility
resource "local_file" "ansible_inventory_ini" {
  filename = "${path.root}/../ansible/inventory/hosts.ini"
  content  = <<-EOT
# NextOpus Ansible Inventory
# Generated by Terraform - DO NOT EDIT MANUALLY

[control_plane]
${module.compute.control_plane_display_name} ansible_host=${module.compute.control_plane_public_ip} private_ip=${module.compute.control_plane_private_ip} k3s_role=server

[workers]
%{for idx, name in module.compute.worker_display_names~}
${name} ansible_host=${module.compute.worker_public_ips[idx]} private_ip=${module.compute.worker_private_ips[idx]} k3s_role=agent worker_index=${idx + 1}
%{endfor~}

[k3s_cluster:children]
control_plane
workers

[k3s_cluster:vars]
ansible_user=ubuntu
ansible_python_interpreter=/usr/bin/python3
ansible_ssh_common_args=-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
%{if module.compute.generated_ssh_private_key_path != null~}
ansible_ssh_private_key_file=${module.compute.generated_ssh_private_key_path}
%{else~}
ansible_ssh_private_key_file=~/.ssh/id_rsa
%{endif~}
  EOT

  file_permission = "0644"

  depends_on = [module.compute]
}
