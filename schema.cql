CREATE TABLE shortn.links (
    key ascii PRIMARY KEY,
    owner ascii,
    url ascii
) WITH COMPACT STORAGE;
CREATE INDEX links_owner_idx ON shortn.links (owner);
