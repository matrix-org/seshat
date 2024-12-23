diesel::table! {
    version_table(id){
        id -> Integer,
        version -> BigInt,
    }
}

diesel::table! {
    reindex_needed_table (id) {
        id -> Integer,
        reindex_needed -> Bool,
    }
}

diesel::table! {
    profile (id) {
        id -> Integer,
        user_id -> Text,
        displayname -> Text,
        avatar_url -> Text,
    }
}

diesel::table! {
    events (id) {
        id -> Integer,
        event_id -> Text,
        sender -> Text,
        server_ts -> BigInt,
        room_id -> Integer,
        #[sql_name = "type"]
        event_type -> Text,
        msgtype -> Text,
        source -> Text,
        profile_id -> Integer,
    }
}
