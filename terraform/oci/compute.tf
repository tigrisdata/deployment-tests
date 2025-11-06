# Render cloud-init from template with the PAR URL
# locals {
#   cloudinit = templatefile("${path.module}/cloudinit.tpl", {
#     par_url           = local.payload_par_url
#     remote_folder     = var.remote_folder_path
#   })
# }

resource "oci_core_instance" "vm" {
  count               = var.instance_count
  compartment_id      = var.compartment_ocid
  display_name        = "${var.display_name_prefix}-${var.region}-${count.index + 1}"
  shape               = var.shape
  availability_domain = "tkeS:US-SANJOSE-1-AD-1"

  # Flex shape config
  dynamic "shape_config" {
    for_each = can(regex(".*Flex$", var.shape)) ? [1] : []
    content {
      ocpus         = var.ocpus
      memory_in_gbs = var.memory_gbs
    }
  }

  source_details {
    source_type = "image"
    source_id    = var.image_ocid
  }

  create_vnic_details {
    subnet_id        = var.subnet_ocid
    assign_public_ip = var.assign_public_ip ? "true" : "false"
  }

# IMDS2 enabled
  instance_options {
    are_legacy_imds_endpoints_disabled = true
  }

  metadata = {
    ssh_authorized_keys = file(var.ssh_public_key_path)
  }
}
