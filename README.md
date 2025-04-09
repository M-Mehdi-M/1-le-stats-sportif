tema 1 - Le Stats Sportif

am implementat un server multi-threaded in Flask care ofera statistici bazate pe date
despre nutritie si obezitate din SUA. arhitectura aplicatiei este modularizata, 
constand din urmatoarele componente principale:

abordarea generala:
DataIngestor - proceseaza datele CSV si ofera metode pentru diferite calcule statistice.
ThreadPool - gestioneaza un thread pool worker care proceseaza joburi in mod asincron.
TaskRunner - implementeaza thread-urile worker care executa joburile si salveaza rezultatele pe disc.
API Endpoints - rutele Flask care expun functionalitatile serverului.

consider ca tema a fost utila pentru a intelege: programare multi-threading si sincronizare,
dezvoltarea API-urilor, procesarea datelor statistice, modelul de job processing asincron si
gestionarea resurselor in aplicatii server.

programul este eficient din punct de vedere: incarcarea CSV-ului o singura data in memorie,
utilizarea unui thread pool pentru a procesa cererile, salvarea rezultatelor pe disc pentru
a simula o baza de date si sincronizarea corecta a thread-urilor prin utilizarea lock-urilor.

exista loc de imbunatatiri: un mecanism mai avansat pentru caching al rezultatelor si adaugarea
unui logging mai detaliat.

implementare
(am lasat comentariu si docstring in fisiere surse mai detaliat)
functionalitati implementate:
Am implementat toate functionalitatile cerute in enunt:
endpoint-uri pentru diverse calcule statistice (de ex: /api/states_mean, /api/best5, etc.)
sistem de job processing asincron
mecanisme de gestionare a job-urilor si rezultatelor
shutdown graceful
unittest si fisiere logging

structura Thread-urilor:

thread-ul principal (Main Thread)
ruleaza serverul Flask si gestioneaza rutele HTTP, primeste si proceseaza request-urile HTTP,
inregistreaza job-uri in coada de procesare, returneaza raspunsuri HTTP catre clienti si 
gestioneaza starea serverului.

thread pool worker (N thread-uri)
numarul de thread-uri este dinamic si depinde de variabila de mediu TP_NUM_OF_THREADS sau este
automat determinat in functie de numarul de CPU-uri disponibile. implementarea se face prin clasa
TaskRunner care extinde Thread. acestor thread-uri sunt responsabile de: extragerea job-urilor din
coada de asteptare job_queue, executarea functiei asociate cu fiecare job, serializarea rezultatului
si salvarea lui pe disc in directorul results, actualizarea dictionarului de status al job-urilor 
si monitorizarea evenimentului shutdown_event pentru a gestiona oprirea graceful.

mecanisme de sincronizare folosite
job_queue : coada thread-safe care stocheaza job-urile ce urmeaza a fi procesate
jobs_lock : lock pentru protejarea accesului concurent la dictionarul de status al job-urilor
counter_lock : lock pentru protejarea incrementarii contorului de job-uri
shutdown_event : event pentru semnalarea inceperii procesului de graceful shutdown

fluxul de executie
clientul face un request la un endpoint (ex: /api/states_mean)
thread-ul principal inregistreaza un job si returneaza un ID
un thread worker preia job-ul din coada si il proceseaza
thread-ul worker salveaza rezultatul pe disc
clientul poate interoga endpoint-ul /api/get_results/<job_id> pentru a obtine rezultatul

resurse utilizate:
https://flask.palletsprojects.com/
https://www.tutorialspoint.com/concurrency_in_python/concurrency_in_python_pool_of_threads.htm
materialele de laborator ASC
