################################################################################
# NextOpus - Root Module Outputs
################################################################################

# ==============================================================================
# Network Outputs
# ==============================================================================

output "vcn_id" {
  description = "OCID of the Virtual Cloud Network"
  value       = module.network.vcn_id
}

output "vcn_cidr" {
  description = "CIDR block of the VCN"
  value       = module.network.vcn_cidr
}

output "public_subnet_id" {
  description = "OCID of the public subnet"
  value       = module.network.public_subnet_id
}

output "private_subnet_id" {
  description = "OCID of the private subnet"
  value       = module.network.private_subnet_id
}

# ==============================================================================
# Security Outputs
# ==============================================================================

output "k3s_nsg_id" {
  description = "OCID of the K3s network security group"
  value       = module.security.k3s_nsg_id
}

# ==============================================================================
# Compute Outputs - Control Plane
# ==============================================================================

output "control_plane_public_ip" {
  description = "Public IP address of the K3s control plane"
  value       = module.compute.control_plane_public_ip
}

output "control_plane_private_ip" {
  description = "Private IP address of the K3s control plane"
  value       = module.compute.control_plane_private_ip
}

# ==============================================================================
# Compute Outputs - Workers
# ==============================================================================

output "worker_public_ips" {
  description = "Public IP addresses of the K3s worker nodes"
  value       = module.compute.worker_public_ips
}

output "worker_private_ips" {
  description = "Private IP addresses of the K3s worker nodes"
  value       = module.compute.worker_private_ips
}

# ==============================================================================
# All Instances
# ==============================================================================

output "all_public_ips" {
  description = "All public IPs (control plane + workers)"
  value       = module.compute.all_public_ips
}

output "all_private_ips" {
  description = "All private IPs (control plane + workers)"
  value       = module.compute.all_private_ips
}

# ==============================================================================
# SSH Access
# ==============================================================================

output "ssh_connection_commands" {
  description = "SSH commands to connect to each instance"
  value = {
    control_plane = "ssh -i ${coalesce(module.compute.generated_ssh_private_key_path, "~/.ssh/id_rsa")} ubuntu@${module.compute.control_plane_public_ip}"
    workers = [
      for idx, ip in module.compute.worker_public_ips :
      "ssh -i ${coalesce(module.compute.generated_ssh_private_key_path, "~/.ssh/id_rsa")} ubuntu@${ip}"
    ]
  }
}

output "ssh_private_key_path" {
  description = "Path to the SSH private key (if auto-generated)"
  value       = module.compute.generated_ssh_private_key_path
}

# ==============================================================================
# Kubernetes Access (after K3s installation)
# ==============================================================================

output "k3s_api_endpoint" {
  description = "K3s API server endpoint (available after K3s installation)"
  value       = "https://${module.compute.control_plane_public_ip}:6443"
}

output "k3s_join_url" {
  description = "K3s join URL for worker nodes (used by Ansible)"
  value       = "https://${module.compute.control_plane_private_ip}:6443"
}

# ==============================================================================
# Ansible Integration
# ==============================================================================

output "ansible_inventory_path" {
  description = "Path to the generated Ansible inventory file"
  value       = abspath("${path.root}/../ansible/inventory/hosts.yml")
}

output "ansible_inventory_data" {
  description = "Structured inventory data for Ansible"
  value       = module.compute.ansible_inventory_data
}

# ==============================================================================
# Quick Start Commands
# ==============================================================================

output "next_steps" {
  description = "Commands to run after Terraform apply"
  value       = <<-EOT

    ============================================================
    NextOpus Infrastructure Deployed Successfully!
    ============================================================

    1. Wait for cloud-init to complete (~3-5 minutes):
       ssh -i ${coalesce(module.compute.generated_ssh_private_key_path, "~/.ssh/id_rsa")} ubuntu@${module.compute.control_plane_public_ip} 'cloud-init status --wait'

    2. Run Ansible to install K3s:
       cd ../ansible
       ansible-playbook -i inventory/hosts.yml playbooks/k3s-install.yml

    3. Access the K3s cluster:
       export KUBECONFIG=~/.kube/nextopus-config
       kubectl get nodes

    Control Plane: ${module.compute.control_plane_public_ip}
    Workers: ${join(", ", module.compute.worker_public_ips)}

    ============================================================
  EOT
}

# ==============================================================================
# Resource Summary
# ==============================================================================

output "resource_summary" {
  description = "Summary of deployed resources"
  value = {
    project_name    = var.project_name
    environment     = var.environment
    region          = var.region
    vcn_cidr        = var.vcn_cidr
    instances_count = 1 + var.worker_config.count
    total_ocpus     = var.control_plane_config.ocpus + (var.worker_config.count * var.worker_config.ocpus)
    total_memory_gb = var.control_plane_config.memory_gb + (var.worker_config.count * var.worker_config.memory_gb)
  }
}
