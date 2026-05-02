################################################################################
# NextOpus - Compute Module
# Provisions 4 ARM (Ampere A1) instances for K3s cluster
# Always Free Tier: 4 OCPUs, 24GB RAM total
################################################################################

# ==============================================================================
# Data Sources
# ==============================================================================

# Get the availability domain
data "oci_identity_availability_domains" "ads" {
  compartment_id = var.compartment_ocid
}

# Get Ubuntu 24.04 ARM image (Canonical)
data "oci_core_images" "ubuntu_arm" {
  compartment_id           = var.compartment_ocid
  operating_system         = "Canonical Ubuntu"
  operating_system_version = "24.04"
  shape                    = "VM.Standard.A1.Flex"
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"
  state                    = "AVAILABLE"
}

# ==============================================================================
# Locals
# ==============================================================================

locals {
  # Use provided image ID or auto-lookup
  os_image_id = var.os_image_id != "" ? var.os_image_id : data.oci_core_images.ubuntu_arm.images[0].id

  # Select first AD - change if needed for your region
  availability_domain = data.oci_identity_availability_domains.ads.availability_domains[0].name

  # Cloud-init template for basic OS setup
  cloud_init_base = <<-EOT
    #cloud-config
    package_update: true
    package_upgrade: true

    packages:
      - curl
      - wget
      - vim
      - htop
      - iotop
      - net-tools
      - jq
      - git
      - apt-transport-https
      - ca-certificates
      - gnupg
      - lsb-release
      - open-iscsi
      - nfs-common
      - python3
      - python3-pip

    write_files:
      - path: /etc/sysctl.d/99-kubernetes.conf
        content: |
          net.bridge.bridge-nf-call-iptables = 1
          net.bridge.bridge-nf-call-ip6tables = 1
          net.ipv4.ip_forward = 1
          net.ipv4.conf.all.forwarding = 1
          net.ipv6.conf.all.forwarding = 1
          vm.swappiness = 0
          vm.overcommit_memory = 1
          kernel.panic = 10
          kernel.panic_on_oops = 1
        permissions: '0644'

      - path: /etc/modules-load.d/k8s.conf
        content: |
          br_netfilter
          overlay
          ip_vs
          ip_vs_rr
          ip_vs_wrr
          ip_vs_sh
          nf_conntrack
        permissions: '0644'

    runcmd:
      # Load kernel modules
      - modprobe br_netfilter
      - modprobe overlay
      - modprobe ip_vs
      - modprobe ip_vs_rr
      - modprobe ip_vs_wrr
      - modprobe ip_vs_sh
      - modprobe nf_conntrack || true

      # Apply sysctl settings
      - sysctl --system

      # Disable swap
      - swapoff -a
      - sed -i '/swap/d' /etc/fstab

      # Enable iscsid for Longhorn (optional storage)
      - systemctl enable --now iscsid

      # Set hostname based on instance name
      - hostnamectl set-hostname $(curl -sH "Authorization: Bearer Oracle" http://169.254.169.254/opc/v2/instance/displayName)

      # Create marker file for Ansible
      - touch /var/lib/cloud/instance/cloud-init-complete

    final_message: "NextOpus node initialized after $UPTIME seconds"
  EOT

  # Common instance configuration
  instance_base_config = {
    compartment_id      = var.compartment_ocid
    availability_domain = local.availability_domain
    shape               = "VM.Standard.A1.Flex"
    subnet_id           = var.subnet_id
    nsg_ids             = var.nsg_ids
    ssh_public_key      = var.ssh_public_key
    image_id            = local.os_image_id
    freeform_tags       = var.freeform_tags
  }
}

# ==============================================================================
# SSH Key Generation (if not provided)
# ==============================================================================

resource "tls_private_key" "ssh" {
  count     = var.ssh_public_key == "" ? 1 : 0
  algorithm = "ED25519"
}

# Save generated private key locally
resource "local_sensitive_file" "ssh_private_key" {
  count    = var.ssh_public_key == "" ? 1 : 0
  content  = tls_private_key.ssh[0].private_key_openssh
  filename = "${path.root}/generated_ssh_key"
  file_permission = "0600"
}

resource "local_file" "ssh_public_key" {
  count    = var.ssh_public_key == "" ? 1 : 0
  content  = tls_private_key.ssh[0].public_key_openssh
  filename = "${path.root}/generated_ssh_key.pub"
  file_permission = "0644"
}

locals {
  # Use provided key or generated key
  effective_ssh_key = var.ssh_public_key != "" ? var.ssh_public_key : tls_private_key.ssh[0].public_key_openssh
}

# ==============================================================================
# Control Plane Instance
# ==============================================================================

resource "oci_core_instance" "control_plane" {
  compartment_id      = var.compartment_ocid
  availability_domain = local.availability_domain
  display_name        = "${var.project_name}-control-plane"
  shape               = "VM.Standard.A1.Flex"

  shape_config {
    ocpus         = var.control_plane_config.ocpus
    memory_in_gbs = var.control_plane_config.memory_gb
  }

  source_details {
    source_type             = "image"
    source_id               = local.os_image_id
    boot_volume_size_in_gbs = var.control_plane_config.boot_volume
  }

  create_vnic_details {
    subnet_id                 = var.subnet_id
    display_name              = "${var.project_name}-control-plane-vnic"
    assign_public_ip          = true
    assign_private_dns_record = true
    hostname_label            = "control-plane"
    nsg_ids                   = var.nsg_ids
  }

  metadata = {
    ssh_authorized_keys = local.effective_ssh_key
    user_data           = base64encode(local.cloud_init_base)
  }

  agent_config {
    is_management_disabled = false
    is_monitoring_disabled = false

    plugins_config {
      name          = "Vulnerability Scanning"
      desired_state = "ENABLED"
    }

    plugins_config {
      name          = "OS Management Service Agent"
      desired_state = "ENABLED"
    }

    plugins_config {
      name          = "Compute Instance Run Command"
      desired_state = "ENABLED"
    }

    plugins_config {
      name          = "Compute Instance Monitoring"
      desired_state = "ENABLED"
    }
  }

  freeform_tags = merge(var.freeform_tags, {
    "Role" = "control-plane"
    "K3s"  = "server"
  })

  lifecycle {
    ignore_changes = [
      source_details[0].source_id, # Ignore image updates
      metadata["user_data"],        # Ignore cloud-init changes
    ]
  }
}

# ==============================================================================
# Worker Instances
# ==============================================================================

resource "oci_core_instance" "workers" {
  count = var.worker_config.count

  compartment_id      = var.compartment_ocid
  availability_domain = local.availability_domain
  display_name        = "${var.project_name}-worker-${count.index + 1}"
  shape               = "VM.Standard.A1.Flex"

  shape_config {
    ocpus         = var.worker_config.ocpus
    memory_in_gbs = var.worker_config.memory_gb
  }

  source_details {
    source_type             = "image"
    source_id               = local.os_image_id
    boot_volume_size_in_gbs = var.worker_config.boot_volume
  }

  create_vnic_details {
    subnet_id                 = var.subnet_id
    display_name              = "${var.project_name}-worker-${count.index + 1}-vnic"
    assign_public_ip          = true
    assign_private_dns_record = true
    hostname_label            = "worker-${count.index + 1}"
    nsg_ids                   = var.nsg_ids
  }

  metadata = {
    ssh_authorized_keys = local.effective_ssh_key
    user_data           = base64encode(local.cloud_init_base)
  }

  agent_config {
    is_management_disabled = false
    is_monitoring_disabled = false

    plugins_config {
      name          = "Vulnerability Scanning"
      desired_state = "ENABLED"
    }

    plugins_config {
      name          = "OS Management Service Agent"
      desired_state = "ENABLED"
    }

    plugins_config {
      name          = "Compute Instance Run Command"
      desired_state = "ENABLED"
    }

    plugins_config {
      name          = "Compute Instance Monitoring"
      desired_state = "ENABLED"
    }
  }

  freeform_tags = merge(var.freeform_tags, {
    "Role"        = "worker"
    "WorkerIndex" = tostring(count.index + 1)
    "K3s"         = "agent"
  })

  lifecycle {
    ignore_changes = [
      source_details[0].source_id,
      metadata["user_data"],
    ]
  }
}

# ==============================================================================
# Wait for Cloud-Init Completion
# ==============================================================================

resource "null_resource" "wait_for_cloud_init" {
  depends_on = [
    oci_core_instance.control_plane,
    oci_core_instance.workers
  ]

  # Trigger on any instance change
  triggers = {
    control_plane_id = oci_core_instance.control_plane.id
    worker_ids       = join(",", oci_core_instance.workers[*].id)
  }

  provisioner "local-exec" {
    command = "echo 'Instances created. Cloud-init will complete in ~3-5 minutes.'"
  }
}
