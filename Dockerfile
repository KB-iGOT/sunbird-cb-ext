FROM eclipse-temurin:8-jdk-jammy

RUN useradd -ms /bin/bash appuser

RUN apt-get update \
    && apt-get install -y \
        curl \
        libxrender1 \
        libjpeg-turbo8 \
        fontconfig \
        libxtst6 \
        xfonts-75dpi \
        xfonts-base \
        xz-utils \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Use the Jammy-specific .deb
RUN curl -L -o wkhtmltox.deb \
      https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6.1-2/wkhtmltox_0.12.6.1-2.jammy_amd64.deb \
 && dpkg -i wkhtmltox.deb \
 && apt-get -f install -y \
 && rm wkhtmltox.deb


COPY sb-cb-ext-0.0.1-SNAPSHOT.jar /opt/

RUN chown -R appuser:appuser /opt
USER appuser
WORKDIR /opt

#HEALTHCHECK --interval=30s --timeout=30s CMD curl --fail http://localhost:7001/actuator/health || exit 1
CMD ["/bin/bash", "-c", "java -XX:+PrintFlagsFinal $JAVA_OPTIONS -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -jar /opt/sb-cb-ext-0.0.1-SNAPSHOT.jar"]

