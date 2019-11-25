FROM ubuntu:16.04
MAINTAINER igf[at]imperial.ac.uk
LABEL maintainer="imperialgenomicsfacility"
ENTRYPOINT []
ENV NB_USER vmuser
ENV NB_GROUP vmuser
ENV NB_UID 1000
USER root
WORKDIR /
RUN useradd -m -s /bin/bash -N -u $NB_UID $NB_USER && \
    groupadd $NB_GROUP && \
    usermod -a -G $NB_GROUP $NB_USER && \
    apt-get -y update &&   \
    apt-get install --no-install-recommends -y \
      locales \
      build-essential && \
    locale-gen en_US.UTF-8 && \
    dpkg-reconfigure locales && \
    apt-get purge -y --auto-remove && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV TINI_VERSION v0.18.0
RUN wget --quiet  https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini && \
    mv tini /usr/local/bin/tini && \ 
    chmod +x /usr/local/bin/tini
USER $NB_USER
WORKDIR /home/$NB_USER
COPY environment.yml /home/$NB_USER/environment.yml
RUN mkdir -p /home/$NB_USER/tmp          
ENV TMPDIR=/home/$NB_USER/tmp
RUN  wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
     bash Miniconda3-latest-Linux-x86_64.sh -b
ENV PATH $PATH:/home/$NB_USER/miniconda3/bin/
RUN conda env create -q -n spark-env --file /home/$NB_USER/environment.yml && \
    echo ". /home/$NB_USER/miniconda3/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "source activate spark-env" >> ~/.bashrc && \
    conda clean -i -t -q -y && \
    rm -rf /home/$NB_USER/.cache && \
    rm -rf /home/$NB_USER/tmp && \
    mkdir -p /home/$NB_USER/tmp && \
    mkdir -p /home/$NB_USER/.cache
 ENTRYPOINT ["/usr/local/bin/tini", "--"]
 CMD ["/bin/bash"]
