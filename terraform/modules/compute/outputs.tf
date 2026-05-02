################################################################################
# NextOpus - Compute Module Outputs
################################################################################

# ==============================================================================
# Control Plane Outputs
# ==============================================================================

output "control_plane_id" {
  description = "OCID of the control plane instance"
  value       = oci_core_instance.control_plane.id
}

output "control_plane_public_ip" {
  description = "Public IP of the control plane"
  value       = oci_core_instance.control_plane.public_ip
}

output "control_plane_private_ip" {
  description = "Private IP of the control plane"
  value       = oci_core_instance.control_plane.private_ip
}

output "control_plane_display_name" {
  description = "Display name of the control plane"
  value       = oci_core_instance.control_plane.display_name
}

# ==============================================================================
# Worker Outputs
# ==============================================================================

output "worker_ids" {
  description = "OCIDs of worker instances"
  value       = oci_core_instance.workers[*].id
}

output "worker_public_ips" {
  description = "Public IPs of worker instances"
  value       = oci_core_instance.workers[*].public_ip
}

output "worker_private_ips" {
  description = "Private IPs of worker instances"
  value       = oci_core_instance.workers[*].private_ip
}

output "worker_display_names" {
  description = "Display names of worker instances"
  value       = oci_core_instance.workers[*].display_name
}

# ==============================================================================
# Combined Outputs for Ansible
# ==============================================================================

output "all_instance_ids" {
  description = "All instance OCIDs (control plane + workers)"
  value = concat(
    [oci_core_instance.control_plane.id],
    oci_core_instance.workers[*].id
  )
}

output "all_public_ips" {
  description = "All public IPs (control plane + workers)"
  value = concat(
    [oci_core_instance.control_plane.public_ip],
    oci_core_instance.workers[*].public_ip
  )
}

output "all_private_ips" {
  description = "All private IPs (control plane + workers)"
  value = concat(
    [oci_core_instance.control_plane.private_ip],
    oci_core_instance.workers[*].private_ip
  )
}

# ==============================================================================
# SSH Key Outputs (if generated)
# ==============================================================================

output "generated_ssh_private_key_path" {
  description = "Path to generated SSH private key (if created)"
  value       = length(tls_private_key.ssh) > 0 ? local_sensitive_file.ssh_private_key[0].filename : null
}

output "generated_ssh_public_key_path" {
  description = "Path to generated SSH public key (if created)"
  value       = length(tls_private_key.ssh) > 0 ? local_file.ssh_public_key[0].filename : null
}

output "ssh_private_key_content" {
  description = "Content of generated SSH private key (sensitive)"
  value       = length(tls_private_key.ssh) > 0 ? tls_private_key.ssh[0].private_key_openssh : null
  sensitive   = true
}

# ==============================================================================
# Structured Output for Ansible Inventory
# ==============================================================================

output "ansible_inventory_data" {
  description = "Structured data for generating Ansible inventory"
  value = {
    control_plane = {
      name       = oci_core_instance.control_plane.display_name
      public_ip  = oci_core_instance.control_plane.public_ip
      private_ip = oci_core_instance.control_plane.private_ip
      id         = oci_core_instance.control_plane.id
    }
    workers = [
      for idx, instance in oci_core_instance.workers : {
        name       = instance.display_name
        public_ip  = instance.public_ip
        private_ip = instance.private_ip
        id         = instance.id
        index      = idx + 1
      }
    ]
  }
}

# ==============================================================================
# Image Information
# ==============================================================================

output "os_image_id" {
  description = "OCID of the OS image used"
  value       = local.os_image_id
}

output "availability_domain" {
  description = "Availability domain where instances are deployed"
  value       = local.availability_domain
}
