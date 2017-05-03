#!/usr/bin/perl -w
# sed -i 's/\\/\//g' *MULT*
# ls *MULT* | ~/git/migraton-tools/bibs_items/stage-HOLDINGS-MULT.pl >> scripts/asset_copy_stage.sql

use strict;

my $first_time = 1;
my $schema;

sub first_time {
    $schema = shift;
    $first_time = 0;
    print qq^
DROP TABLE IF EXISTS m_$schema.asset_copy_multi_legacy;
CREATE TABLE m_$schema.asset_copy_multi_legacy (
    eg_bib_id BIGINT,
    eg_copy_id INTEGER,
    hseq TEXT,
    subfield TEXT,
    value TEXT
);
CREATE INDEX ON m_$schema.asset_copy_multi_legacy (eg_bib_id);
CREATE INDEX ON m_$schema.asset_copy_multi_legacy (eg_copy_id);
CREATE INDEX ON m_$schema.asset_copy_multi_legacy (hseq);
CREATE INDEX ON m_$schema.asset_copy_multi_legacy (subfield);
CREATE INDEX ON m_$schema.asset_copy_multi_legacy (hseq,subfield);\n\n
^;

}


while (my $line = <>) {
    chomp $line;
    if ($line =~ /^(.+?)-.+(.)\.pg$/) {
        first_time($1) if $first_time;
        print "\\COPY m_$1.asset_copy_multi_legacy (eg_bib_id,hseq,value) FROM '$line'\n";
        print "UPDATE m_$1.asset_copy_multi_legacy SET subfield = '$2' WHERE subfield IS NULL;\n\n";
    }
}

print "UPDATE m_$schema.asset_copy_multi_legacy SET eg_copy_id = b.id FROM m_$schema.asset_copy_legacy b WHERE x_eg_bib_id = eg_bib_id AND x_hseq = hseq;\n\n";

