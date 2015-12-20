create table articles (
       id serial primary key,
       wid integer not null unique,
       title text not null,
       content text not null,
       vector text,
       affinity_count integer not null default 0,
       preference integer not null default 0,
       compressed_vector integer[],
       cvector integer[]
);

create table affinity (
       aid integer,
       bid integer,
       score double precision
);
       
create index articles_wid_index on articles (wid);
create index articles_affinity_count_index on articles (affinity_count);

create or replace function update_compressed_vectors(
  wid0 integer, vector0 integer[],
  wid1 integer, vector1 integer[],
  wid2 integer, vector2 integer[],
  wid3 integer, vector3 integer[],
  wid4 integer, vector4 integer[],
  wid5 integer, vector5 integer[],
  wid6 integer, vector6 integer[],
  wid7 integer, vector7 integer[],
  wid8 integer, vector8 integer[],
  wid9 integer, vector9 integer[],
  wid10 integer, vector10 integer[],
  wid11 integer, vector11 integer[],
  wid12 integer, vector12 integer[],
  wid13 integer, vector13 integer[],
  wid14 integer, vector14 integer[],
  wid15 integer, vector15 integer[],
  wid16 integer, vector16 integer[],
  wid17 integer, vector17 integer[],
  wid18 integer, vector18 integer[],
  wid19 integer, vector19 integer[]
) returns boolean as $$
begin
  update articles set compressed_vector = vector0 where wid = wid0;
  update articles set compressed_vector = vector1 where wid = wid1;
  update articles set compressed_vector = vector2 where wid = wid2;
  update articles set compressed_vector = vector3 where wid = wid3;
  update articles set compressed_vector = vector4 where wid = wid4;
  update articles set compressed_vector = vector5 where wid = wid5;
  update articles set compressed_vector = vector6 where wid = wid6;
  update articles set compressed_vector = vector7 where wid = wid7;
  update articles set compressed_vector = vector8 where wid = wid8;
  update articles set compressed_vector = vector9 where wid = wid9;
  update articles set compressed_vector = vector10 where wid = wid10;
  update articles set compressed_vector = vector11 where wid = wid11;
  update articles set compressed_vector = vector12 where wid = wid12;
  update articles set compressed_vector = vector13 where wid = wid13;
  update articles set compressed_vector = vector14 where wid = wid14;
  update articles set compressed_vector = vector15 where wid = wid15;
  update articles set compressed_vector = vector16 where wid = wid16;
  update articles set compressed_vector = vector17 where wid = wid17;
  update articles set compressed_vector = vector18 where wid = wid18;
  update articles set compressed_vector = vector19 where wid = wid19;
  return true;
end;
$$ language plpgsql;
