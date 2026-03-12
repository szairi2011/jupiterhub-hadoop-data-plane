# JupyterHub 4.1.6 — the hub process only; it does NOT serve notebooks itself.
# Notebooks run in separate singleuser containers spawned by DockerSpawner.
FROM quay.io/jupyterhub/jupyterhub:4.1.6

# dockerspawner: creates/destroys per-user containers via the Docker socket.
#   Pin to 13.x for JupyterHub 4.x API compatibility.
# jupyterhub-dummyauthenticator: accepts any username with a shared password.
#   DEV ONLY — replace with PAMAuthenticator/LDAPAuthenticator in production.
#   Latest available version is 0.3.1 (package was not updated for JupyterHub 4,
#   but still works because the Authenticator base-class API is stable).
RUN pip install --no-cache-dir \
        dockerspawner==13.0.0 \
        jupyterhub-dummyauthenticator==0.3.1
