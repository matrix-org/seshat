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
