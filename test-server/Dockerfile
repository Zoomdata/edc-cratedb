# This Docker image is used for starting up a small crate.io instance containing the Zoomdata
# reference data set for testing connector servers. It is in no way intended for production use
# or follows best practices for configuring and running crate

FROM debian:8.6
MAINTAINER support@zoomdata.com

# Install prerequisite libraries and tools
RUN apt-get -y update
RUN apt-get -y install init-system-helpers apt-transport-https

# Install Java 8 (required for Crate)
RUN echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu precise main" | tee -a /etc/apt/sources.list
RUN echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu precise main" | tee -a /etc/apt/sources.list

# Port 80 to avoid firewall blocking issues
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886
RUN apt-get -y update
RUN echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
RUN echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections
RUN apt-get -y install oracle-java8-installer

# Add Crate repository
RUN wget https://cdn.crate.io/downloads/apt/DEB-GPG-KEY-crate
RUN apt-key add DEB-GPG-KEY-crate

# Crate stable releases only
RUN echo "deb http://http.debian.net/debian jessie-backports main" >> /etc/apt/sources.list
RUN echo "deb https://cdn.crate.io/downloads/apt/stable/ jessie main" >> /etc/apt/sources.list
RUN apt-get -y update

# Control version of Crate to ensure image does not break. Manually upgrade after verification
RUN apt-get -y install crate

# Add our data to the image - this is not at all a best practice but easier to bootstrap for our purposes
# Docker will automatically unpack this
ADD crate-backup.tar.gz /var/lib/crate
RUN chown -R crate:crate /var/lib/crate
RUN echo "network.host: _site_" >> /etc/crate/crate.yml

# Expose default Crate ports
EXPOSE 4200 4300 5432

RUN touch /var/log/crate/crate.log
RUN chown crate:crate /var/log/crate/crate.log

CMD service crate start && tail -F /var/log/crate/crate.log
