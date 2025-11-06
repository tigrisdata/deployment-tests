output "instance_ids" {
  value = [for i in oci_core_instance.vm : i.id]
}

output "private_ips" {
  value = [for i in oci_core_instance.vm : i.private_ip]
}

output "public_ips" {
  description = "Empty if assign_public_ip=false."
  value       = [for i in oci_core_instance.vm : try(i.public_ip, "") ]
}
