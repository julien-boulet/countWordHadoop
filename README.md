# countWord

## Summary

- [The statement](#The-statement)
- [Run it](#Run-it)

## The statement

Compter le nombre d'occurence de chaque mot et afficher le top 100, en rendant le code distribuable (se baser sur le pattern MapReduce).
Rendu attendu: code source Java

J'ai découpé le fichier les3moustquetaires.txt en trois pour avoir trois mappers dans le job1. Ils sont dans le dossier files

Le docker-compose est auto-porteur. Le dockerfile build le jar avec gradle et le rajoute dans une image custom de namenode.
Les 3 fichiers textes et le script de lancement du job sont montés dans le namenode grace a des volumes.

## Run it

### With Docker :

#### Prerequisites

```bash
Docker
Docker-compose
```

#### Command line

```bash
docker-compose up

docker exec -it namenode bash

sh /script/script.sh
```

### With Java and Gradle

#### Prerequisites

| Software | Version |
| -------- | ------- |
| Gradle   | 5       |
| JAVA     | 8      |

#### Command line

```bash
# Compilation
./gradlew clean build
```


