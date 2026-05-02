################################################################################
# NextOpus - Security Module Outputs
################################################################################

output "public_security_list_id" {
  description = "OCID of the public security list"
  value       = oci_core_security_list.public.id
}

output "private_security_list_id" {
  description = "OCID of the private security list"
  value       = oci_core_security_list.private.id
}

output "k3s_nsg_id" {
  description = "OCID of the K3s network security group"
  value       = oci_core_network_security_group.k3s.id
}

output "k3s_nsg_ids" {
  description = "List of K3s NSG IDs for instance attachment"
  value       = [oci_core_network_security_group.k3s.id]
}
