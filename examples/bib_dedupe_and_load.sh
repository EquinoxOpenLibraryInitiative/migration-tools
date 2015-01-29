#!/bin/bash

# Copyright (C) 2009-2014 Equinox Software, Inc.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# This script provides an example of how to
# process a file of MARC bib records and deduplicate
# them against bibs already present in an Evergreen
# database.  The result in the output directory will
# be three files:
#
#   bibs_to_add.xml - new bibs in MARCXML, converted to UTF8,
#                     with record IDs set.  This is the set
#                     of bibs that do NOT match incumbent bibs,
#                     and which therefore should be loaded.
#   old2new.map     - two-column mapping from old ILS bib ID
#                     to pre-merge Evergreen bib ID
#   merge.final     - for the bibs that match ones already in
#                     the Evergreen database, a two-column mapping
#                     whose first column is the incumbent bib's
#                     Evergreen ID and whose second column is the
#                     placeholder Evergreen bib ID assigned to the
#                     bib in the input file.  Joining old2new.map
#                     and merge.final will produce a mapping from the
#                     original ILS bib ID to the destination Evergreen
#                     bib ID.
#
# Since bibs_to_add.xml contains the Evergreen bib ID to use when
# loading the bib record in the 903 field, the --idfield=903 option
# should be used when processing that file through marc2bre.pl.

# First, some initial settings

BIBFILE=/path/to/bib.mrc    # file of MARC bib records
BIBCHARSET=MARC8            # character encoding of the bib records, typically
                            # either MARC8 or UTF8
BIBIDSTART=1000000          # starting bib ID to assign to new bibs
ORIGIDTAG=999               # MARC tag storing the original ILS's bib ID
ORIGIDSF=a                  # MARC subfield storing the original ILS's bib ID

# connection parameters for the Evergreen database
DBHOST=localhost
DBPORT=5432
DBNAME=evergreen
DBUSER=evergreen
DBPASS=evergreen

INTER=scratch               # directory to store intermediate files
LOG=log                     # directory to store log files
OUT=out                     # directory to store output files

MIGTOOLS=$HOME/migration-tools  # path to Git checkout of migration-tools

export PATH=$PATH:$MIGTOOLS:/openils/bin
export PERL5LIB=$PERL5LIB:$MIGTOOLS/Equinox-Migration/lib

# This function converts the source bib file to MARCXML,
# runs it through a cleanup process, and emits a mapping file
# from the source ILS bib ID to the (as-yet-undedupped) new Evergreen
# bib ID.
function prepare_bibs {

    if [ ! -r $BIBFILE ]
    then
        echo ERROR: Could not read bib file $BIBFILE
        exit 1
    fi

    echo Running yaz-marcdump:
    yaz-marcdump -f $BIBCHARSET -t UTF-8 -l 9=97 -o marcxml $BIBFILE > $INTER/bibs_pass1.xml
    echo yaz-marcdump is done

    echo Running marc_cleanup:
    pushd $INTER
    marc_cleanup --marcfile=$INTER/bibs_pass1.xml --fullauto \
        -o $INTER/bib.clean.xml -x $LOG/bib.precleanup.errors.xml --renumber-from $BIBIDSTART \
        -ot $ORIGIDTAG -os $ORIGIDSF
    popd
    echo marc_cleanup is done

    # old2new.map is the source ILS bib ID to new Evergreen bib ID map
    cp $INTER/old2new.map $OUT/old2new.map

}

# This function calculates "fingerprints" for all of the
# bibs in the input file as well as the bibs in the Evergreen
# database, then uses those fingerprints to identify duplicate
# records.  The result is a file of bibs in MARCXML format that
# should be laoded into the database.
function calculate_duplicates {

    echo "select id || chr(9) || REGEXP_REPLACE(marc, E'\\n','','g') from biblio.record_entry where not deleted and id < $BIBIDSTART" > $INTER/incumbent_bibs.sql

    echo Extracting incumbent bibs:
    PGPASSWORD=$DBPASS psql -h $DBHOST -A -t -U $DBUSER $DBNAME < $BIN/incumbent_bibs.sql | munge_marc_export_for_fingerprint.pl > $INTER/incumbent.mrc

    date
    echo fingerprinter on incumbent bibs:
    fingerprinter --fingerprints oclc,isbn,edition,issn,lccn,accomp,authpub \
        -o $INTER/incumbent.fp -x $INTER/incumbent.fp.ex $INTER/incumbent.mrc

    date
    echo fingerprinter on new bibs:
    fingerprinter --fingerprints oclc,isbn,edition,issn,lccn,accomp,authpub \
        -o $INTER/new.fp -x err/new.fp.ex $INTER/bib.clean.xml
    
    date
    echo Merging fingerprints:
    echo ...all
    cat $INTER/incumbent.fp $INTER/new.fp | sort -r > $INTER/dedupe.fp
    match_fingerprints -t $BIBIDSTART -o $INTER/merge $INTER/dedupe.fp

    for i in isbn authpub lccn oclc issn edition
    do
        echo ...$i
        grep $i $INTER/dedupe.fp > $INTER/$i.fp
        match_fingerprints -t $BIBIDSTART -o $INTER/merge-$i $INTER/$i.fp
    done

    echo ...combining all of the above
    cat $INTER/merge $INTER/merge-isbn $INTER/merge-authpub \
        $INTER/merge-lccn  $INTER/merge-oclc $INTER/merge-edition | \
    sort | uniq > $INTER/merge-combined
    cleanup_merge_map.pl $INTER/merge-combined > $OUT/merge.final

    echo Dedupe merge map: $OUT/merge.final

    echo extract_loadset:
    extract_loadset -l 1 -i $INTER/bib.clean.xml -o $OUT/bibs_to_add.xml $OUT/merge.final

    echo Done with fingerprinting.
    date

}

# actually run the process
prepare_bibs
calculate_duplicates

