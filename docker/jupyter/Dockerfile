FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

# Install SparkMagic and register kernels
# Notes:
#   - jupyter nbextension / serverextension were removed in JupyterLab 4; use "jupyter server extension"
#   - ipywidgets 8+ auto-registers in JupyterLab 4; no manual nbextension enable needed
#   - --sys-prefix (not --user) installs kernels system-wide so jovyan can see them
RUN pip install --no-cache-dir sparkmagic==0.23.0 && \
    jupyter server extension enable --py sparkmagic && \
    jupyter kernelspec install --sys-prefix \
        "$(python -c 'import sparkmagic, os; print(os.path.dirname(sparkmagic.__file__))')/kernels/pysparkkernel" && \
    jupyter kernelspec install --sys-prefix \
        "$(python -c 'import sparkmagic, os; print(os.path.dirname(sparkmagic.__file__))')/kernels/sparkkernel" && \
    jupyter kernelspec install --sys-prefix \
        "$(python -c 'import sparkmagic, os; print(os.path.dirname(sparkmagic.__file__))')/kernels/sparkrkernel"

# Pre-create all directories that Jupyter and SparkMagic need at runtime.
# Must be done as root and chowned to jovyan before switching user,
# otherwise jovyan cannot create subdirectories inside root-owned parents.
RUN mkdir -p \
        /home/jovyan/.sparkmagic \
        /home/jovyan/.local/share/jupyter/runtime \
        /home/jovyan/.local/share/jupyter/kernels \
        /home/jovyan/.ipython && \
    chown -R jovyan:users /home/jovyan/.sparkmagic /home/jovyan/.local /home/jovyan/.ipython

USER jovyan
