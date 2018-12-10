# vim:set ft=dockerfile:
FROM postgres:11

RUN apt-get update
RUN apt-get install -y postgresql-plperl-11

EXPOSE 5432
CMD ["postgres"]
