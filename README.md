# Elephant Replicant

<p>
  <img src="https://github.com/ttab/elephant-replicant/raw/main/docs/elephant-replicant.png?raw=true" width="256" alt="Elephant Replicant">
</p>

Replicates data to another Elephant environment. The replicant uses optimistic locking to prevent overwrites of documents that have been modified in the destination. This is not replication as a method of providing a backup or standby instance, rather it's a solution for keeping a stage or QA environment updated with relevant data.

ACL:s will always be replicated.

Attachments will only be replicated if `-all-attachments` is set or if they have been explicitly enabled by document type and attachment name using `-include-attachments`.

## Example configuration

Configuration for running replication to a local repository instance:

``` shell
ADDR=:1280
PROFILE_ADDR=:1281

IGNORE_TYPES=core/article+meta,tt/wire,tt/wire-provider,tt/wire-source
IGNORE_SUBS=core://application/elephant-wires
#INCLUDE_ATTACHMENTS=image.core/image,laygout.tt/print-layout
ALL_ATTACHMENTS=true

# Replicate from prod
REPOSITORY_ENDPOINT=https://repository.api.tt.ecms.se
OIDC_CONFIG=https://login.tt.se/realms/elephant/.well-known/openid-configuration
CLIENT_ID=replicant-send
CLIENT_SECRET=xoxo

# Replicate from stage
# REPOSITORY_ENDPOINT=https://repository.stage.tt.se
# OIDC_CONFIG=https://login.stage.tt.se/realms/elephant/.well-known/openid-configuration
# CLIENT_ID=replicant-send
# CLIENT_SECRET=xoxo

TARGET_REPOSITORY_ENDPOINT=http://localhost:1080
TARGET_OIDC_CONFIG=https://login.stage.tt.se/realms/elephant/.well-known/openid-configuration
TARGET_CLIENT_ID=replicant-receive
TARGET_CLIENT_SECRET=xoxo
```
