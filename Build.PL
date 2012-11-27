#!/usr/bin/perl

use Module::Build;

my $build = Module::Build->new(
    module_name => "AnyEvent::Memcached::Hash::Consistent",
    version_from => "AnyEvent::Memcached::Hash::Consistent",
    requires    => {
        "AnyEvent::Memcached" => 0,
        "Set::ConsistentHash"     => 0,
    },
);

$build->create_build_script;
