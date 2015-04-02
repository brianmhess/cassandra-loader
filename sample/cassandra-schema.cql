create keyspace titanic WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use titanic;

CREATE TABLE survivors (
 id varchar,
 survived boolean,
 passenger_class int,
 name varchar,
 sex varchar,
 age int,
 num_siblings_spouse int,
 num_parents_children int,
 ticket_id varchar,
 fare varchar,
 cabin varchar,
 port_of_embarkation varchar,
 primary key (id));