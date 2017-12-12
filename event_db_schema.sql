create table
event (
    agent_id bigint,
    repo_id bigint,
    ltime bigint, -- logical time
    rtime bigint, -- real time, unix timestamps
    event_type text,
    payload text
);

create index
idx1 on event (agent_id, repo_id);
create index
idx2 on event (repo_id);
