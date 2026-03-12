# Per-user JupyterLab image spawned by DockerSpawner.
# Extends the official pyspark-notebook so local PySpark still works as a fallback;
# in normal use all code runs remotely via SparkMagic → Livy.
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

# SparkMagic install notes:
#   - `jupyter server extension enable` (not the old `serverextension` subcommand,
#     which was removed in JupyterLab 4)
#   - `--sys-prefix` installs kernelspecs into the conda env so jovyan can see them;
#     `--user` would install into root's home directory, invisible to jovyan
RUN pip install --no-cache-dir sparkmagic==0.23.0 && \
    jupyter server extension enable --py sparkmagic && \
    jupyter kernelspec install --sys-prefix \
        "$(python -c 'import sparkmagic, os; print(os.path.dirname(sparkmagic.__file__))')/kernels/pysparkkernel" && \
    jupyter kernelspec install --sys-prefix \
        "$(python -c 'import sparkmagic, os; print(os.path.dirname(sparkmagic.__file__))')/kernels/sparkkernel" && \
    jupyter kernelspec install --sys-prefix \
        "$(python -c 'import sparkmagic, os; print(os.path.dirname(sparkmagic.__file__))')/kernels/sparkrkernel"

# Pre-create dirs as root then chown to jovyan.
# If these dirs don't exist when the container starts, Jupyter tries to create them
# inside a root-owned parent and fails with "Permission denied".
RUN mkdir -p \
        /home/jovyan/.sparkmagic \
        /home/jovyan/shared-notebooks \
        /home/jovyan/.local/share/jupyter/runtime \
        /home/jovyan/.local/share/jupyter/kernels \
        /home/jovyan/.ipython && \
    chown -R jovyan:users /home/jovyan/.sparkmagic /home/jovyan/shared-notebooks \
                          /home/jovyan/.local /home/jovyan/.ipython

# Bake in the SparkMagic Livy config and the shared notebooks.
# If DockerSpawner volume-mounts succeed at runtime (REPO_ROOT correctly set),
# Docker overlays the live files on top and users see the latest version.
# If the mounts fail for any reason, these image-baked defaults ensure the
# container still works out of the box — no broken SparkMagic, no empty lab.
#
# NOTE: build context must be the repo root (set in docker-compose.yml) so
# that COPY can reach config/ and notebooks/.
COPY --chown=jovyan:users config/sparkmagic/config.json /home/jovyan/.sparkmagic/config.json
COPY --chown=jovyan:users notebooks/ /home/jovyan/shared-notebooks/

USER jovyan
