## Namespace needed for Object Storage resources
#data "oci_objectstorage_namespace" "ns" {
#  compartment_id = var.compartment_ocid
#}

## Package the local folder into a .tgz locally during plan/apply.
## We keep it inside .terraform-local/ (gitignored) to avoid clutter.
#locals {
#  payload_dir  = "${path.module}/.terraform-local"
#  payload_tgz  = "${local.payload_dir}/payload.tgz"
#  payload_name = "payload.tgz"
#}

#resource "null_resource" "ensure_payload_dir" {
#  provisioner "local-exec" {
#    command = "mkdir -p ${local.payload_dir}"
#  }
#}

#data "archive_file" "folder_tgz" {
#  depends_on = [null_resource.ensure_payload_dir]
#  type        = "tgz"
#  source_dir  = var.local_folder
#  output_path = local.payload_tgz
#}
