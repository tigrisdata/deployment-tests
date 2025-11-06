# #cloud-config
# package_update: true
# runcmd:
#   - [ sh, -lc, "mkdir -p ${remote_folder}" ]
#   - [ sh, -lc, "curl -fSL '${par_url}' -o /var/tmp/payload.tgz" ]
#   - [ sh, -lc, "tar -xzf /var/tmp/payload.tgz -C ${remote_folder}" ]
#   - [ sh, -lc, "rm -f /var/tmp/payload.tgz" ]
#   - [ sh, -lc, "chown -R opc:opc ${remote_folder} || true" ]
