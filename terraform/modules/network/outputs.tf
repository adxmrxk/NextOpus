################################################################################
# NextOpus - Network Module Outputs
################################################################################

output "vcn_id" {
  description = "OCID of the VCN"
  value       = oci_core_vcn.main.id
}

output "vcn_cidr" {
  description = "CIDR block of the VCN"
  value       = oci_core_vcn.main.cidr_blocks[0]
}

output "public_subnet_id" {
  description = "OCID of the public subnet"
  value       = oci_core_subnet.public.id
}

output "public_subnet_cidr" {
  description = "CIDR block of public subnet"
  value       = oci_core_subnet.public.cidr_block
}

output "private_subnet_id" {
  description = "OCID of the private subnet"
  value       = oci_core_subnet.private.id
}

output "private_subnet_cidr" {
  description = "CIDR block of private subnet"
  value       = oci_core_subnet.private.cidr_block
}

output "internet_gateway_id" {
  description = "OCID of the Internet Gateway"
  value       = oci_core_internet_gateway.main.id
}

output "public_route_table_id" {
  description = "OCID of the public route table"
  value       = oci_core_route_table.public.id
}

output "private_route_table_id" {
  description = "OCID of the private route table"
  value       = oci_core_route_table.private.id
}
