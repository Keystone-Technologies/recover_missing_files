use 5.010;
use strict;
use warnings;
use Mojo::Loader 'load_class';
use File::Basename;
use Getopt::Long qw(GetOptionsFromArray :config no_auto_abbrev no_ignore_case);
use Time::HiRes qw(gettimeofday tv_interval);
use Time::Progress;

use constant {WRITE => 1, NO_WRITE => 0};

#my $max_bind_size = 65_535;
my $max_bind_size = 15_000;

my $images = qr(\.tif$);

GetOptions(
  'S=i' => \my $S,           # Grab a sampling of the full data set
  's=i' => \my $s,           # Create a sampling of that sampling
  'a=i' => \my $files_abort, # Max records to process
  'A=i' => \my $abort,       # Max records to process
);

die "DBI not defined\n" unless $ENV{DBI};
my %db_type = (
  'postgresql' => ['Pg' => "postgresql://postgres\@/%1"],
  'mysql' => ['mysql' => "mysql://%1:%1\@/%1"],
);
my ($db_type) = ($ENV{DBI} =~ /^(\w+)/);
my $class = "Mojo::".($db_type{$db_type}->[0]||$db_type);
my $e = load_class $class;
warn qq{Loading "$class" failed: $e} and next if ref $e;
if ( $ENV{DBI} =~ /:\/\/([^\/]+)$/ ) { $_ = $1; $db_type{$db_type}->[1] =~ s/%1/$_/g }
else { $db_type{$db_type}->[1] = $ENV{DBI} }
my $dbi = $class->new($db_type{$db_type}->[1]);
my $db = $dbi->db;

if ( my $file = $ARGV[0] ) {
  if ( $file !~ /(original|backups|recovery)/ ) {
    sample($file, $S, 'sample.txt') if $S;
    if ( -e 'sample.txt' && $s ) {
      sample('sample.txt', $s, 'original.txt');
      sample('original.txt', $s*.9, 'backups.txt');
      sample('original.txt', $s*.8, 'recovery.txt');
      print qx(wc -l [a-z]*.txt);
      foreach my $file (qw(original.txt backups.txt recovery.txt)) { load($file) }
    } else {
      load($file => 'original');
    }
  } else {
    load($file);
  }
} else {
  # select 'original' t,count(*) c from original union select 'backups' t,count(*) c  from backups union select 'recovery' t,count(*) c  from recovery;
  # select count(*) 'missing from backups' from original where filename not in (select filename from backups);  
  # select count(*) 'missing from recovery' from original where filename not in (select filename from recovery);
  # select count(*) 'missing from backups but available in recovery' from recovery where filename in 
  #   (select filename from original where filename not in (select filename from backups)) order by d;
  # select count(*) 'missing from backups but available in recovery', (count(*)+(select count(*) from backups))/(select count(*) from original)*100 'percent fully recovered'
  #   from recovery where filename in (select filename from original where filename not in (select filename from backups)) order by d;
  # missing_from_backups_but_available_in_recovery
  my $recovery = $db->query('select * from recovery where filename in (select filename from original where filename not in (select filename from backups)) order by d');
  while ( my $file = $recovery->hash ) {
    printf "mkdir '%s'\n", $file->{path};
    printf "copy '%s' '%s'\n", $file->{filename}, $file->{path};
  }
}

sub sample {
  my ($in, $count, $out) = @_;
  return unless -e $in;
  $count = int($count);

  my $size = -s $in;

  open(IN,$in) || die "Can't open $in\n";
  open(OUT,">$out") || die "Can't open $out\n";

  print "$in ($count) > $out\n";
  while ($count--) {
    printf "  %08d\r", $count;
    seek(IN,int(rand($size)),0);
    $_=readline(IN);                         # ignore partial line
    redo unless defined ($_ = readline(IN)); # catch EOF
    print OUT $_;
  }
  print "\n";

  close OUT;
  close IN;
}

sub load {
  my ($file, $table) = @_;
  return unless -e $file;
  $|=1;
  $table ||= $file;
  $table =~ s/\.\w+$//;
  my $files = 0;
  my $c = 0;
  my $skipped = 0;
  print "Preparing $table... ";
  my $mig = lc(ref $dbi);
  $mig =~ s/.+:://;
  $dbi->migrations->name("${mig}_$table")->from_data->migrate;
  $db->query("truncate table $table");
  $db->query("ALTER SEQUENCE ${table}_id_seq RESTART WITH 1") if ref $dbi =~ /pg/i;
  $db->query("alter table $table auto_increment = 1") if ref $dbi =~ /mysql/i;
  say "Done!";
  my @buffer = ();
  print "Counting records in $file... ";
  open IN, $file;
  #1 while <IN>;
  my $lines = 12359817||$.;
  $lines = $abort || $lines;
  say $lines;
  print "Getting batch size... ";
  my $batch = adjust_batch(int($lines*.1));
  say $batch;
  seek(IN,0,0);
  print "$file => $table\n";
  my $dump = 0;
  my $buffer = [gettimeofday];
  my $start = [gettimeofday];
  my $_start = 0;
  my $p = Time::Progress->new(min => 1, max => $lines, smoothing => 1);
  my $format = sub {
    my $format = "\r%1 %p  Elapsed: %L  Buffer: %2 / %3 Processed: %4(S) / %5(A) / %6(P) /%7(T) Read/Write time: %8/%9";
    my @format = (
      shift,
      sprintf("% 5d", shift), # buffer
      sprintf("% 5d", shift),         # batch
      sprintf("% 8d", shift),         # skipped
      sprintf("% 8d", shift),         # added
      sprintf("% 8d", shift),         # processed
      sprintf("% 8d", shift),         # total
      sprintf("%05.2f", shift),       # read time
      sprintf("%05.2f", shift)        # write time
    );
    my $n = 0;
    foreach ( @format ) {
      $n++;
      s/(\d)(?=(\d{3})+(\D|$))/$1\,/g if length > 3 && !/\./;
      $format =~ s/%$n/$_/g;
    }
    return $format;
  };
  while ( local $_ = <IN> ) {
    last if $abort && $c >= $abort;
    last if $files_abort && $files >= $files_abort;
    print STDERR $p->report($format->(' ', buffer_size($#buffer), $batch, $skipped, $files, $c, $lines, tv_interval($buffer), $dump), $c) if !$_start || int(tv_interval($start)) != int(tv_interval($_start));
    $_start = $start;
    $c++;
    s/\s*$//;
    ++$skipped and next if /\\$/;
    my ($date) = (/\\(\d{8})\\/);
    my ($path, $file) = (/(.+\\)(.+)/);
    ++$skipped and next unless $date && lc($file) =~ $images;
    push @buffer, $date, ($path||''), ($file||'');
    next unless buffer_size($#buffer) == $batch;
    $buffer = [gettimeofday];
    print STDERR $p->report($format->('-', buffer_size($#buffer), $batch, $skipped, $files, $c, $lines, tv_interval($buffer), $dump), $c);
    ($files, $dump) = dump_buffer(\@buffer => $table => $files);
    $batch = adjust_batch($batch => $dump);
  }
  close IN;
  print STDERR $p->report($format->(' ', buffer_size($#buffer), $batch, $skipped, $files, $c, $lines, tv_interval($buffer), $dump), $c);
  if ( @buffer ) {
    $batch = adjust_batch(\@buffer);
    print STDERR $p->report($format->('-', buffer_size($#buffer), $batch, $skipped, $files, $c, $lines, tv_interval($buffer), $dump), $c);
    ($files, $dump) = dump_buffer(\@buffer => $table => $files);
    $buffer = [gettimeofday];
  }
  print STDERR $p->report($format->(' ', buffer_size($#buffer), $batch, $skipped, $files, $c, $lines, tv_interval($buffer), $dump), $c);
  print "\n";
}

sub buffer_size { ((shift()+1)/3) }
sub dump_buffer {
  my ($buffer, $table, $files) = @_;
  my $insert_values = insert_values(qw(d path filename) => buffer_size($#$buffer));
  my $dump = [gettimeofday];
  my $sleep = "select %s \"files\"";
  $sleep = "select %s \"files\" from pg_sleep(0)" if ref $dbi =~ /pg/i;
  $sleep = "select %s \"files\", sleep(0)" if ref $dbi =~ /mysql/i;
  $files += WRITE
    #? buffer_size($#$buffer)
    ? $db->query("insert into $table $insert_values", @$buffer)->rows
    #: buffer_size($#$buffer)
    : $db->query(sprintf($sleep, buffer_size($#$buffer)))->hash->{files};
  ;
  @$buffer = ();
  return $files, tv_interval($dump);
}

sub insert_values { my $buffer = pop; sprintf "(%s) values %s", join(',', @_), join(',', map { '(?,?,?)' } 1..$buffer) }
sub adjust_batch {
  my ($batch, $dump) = @_;
  $dump //= 0;
  return buffer_size($#$batch) if ref $batch;
  #return $batch;
  $batch = int($batch*.9) while !$dump && $batch > $max_bind_size/3;
  $batch = int($batch*.9) if $dump && $batch > $max_bind_size/3 || $dump > 3;
  $batch = int($batch*1.1) if $dump && $batch <= $max_bind_size/3 && $dump < 1;
  $batch = 1 if $batch < 1;
  return $batch;
}

Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

__DATA__
@@ pg_recovery
-- 1 up
create table recovery (id serial primary key, d date, path varchar(1024), filename varchar(64), mtime timestamp, size int);
create index idx_d on recovery (d);
create index idx_path on recovery (path);
create index idx_filename on recovery (filename);

-- 1 down
drop table recovery;

@@ pg_backups
-- 1 up
create table backups (id serial primary key, d date, path varchar(1024), filename varchar(64), mtime timestamp, size int);
create index idx_d on backups (d);
create index idx_path on backups (path);
create index idx_filename on backups (filename);

-- 1 down
drop table backups;

@@ pg_original
-- 1 up
create table original (id serial primary key, d date, path varchar(1024), filename varchar(64), mtime timestamp, size int);
create index idx_d on original (d);
create index idx_path on original (path);
create index idx_filename on original (filename);

-- 1 down
drop table original;

@@ mysql_recovery
-- 1 up
create table recovery (id int primary key auto_increment, d date, path varchar(1024), filename varchar(64), mtime datetime, size int, status enum('san', 'usb', 'missing'));
alter table recovery add index idx_d (d);
alter table recovery add index idx_path (path);
alter table recovery add index idx_filename (filename);

-- 1 down
drop table recovery;

@@ mysql_backups
-- 1 up
create table backups (id int primary key auto_increment, d date, path varchar(1024), filename varchar(64), mtime datetime, size int, status enum('san', 'usb', 'missing'));
alter table backups add index idx_d (d);
alter table backups add index idx_path (path);
alter table backups add index idx_filename (filename);

-- 1 down
drop table backups;

@@ mysql_original
-- 1 up
create table original (id int primary key auto_increment, d date, path varchar(1024), filename varchar(64), mtime datetime, size int, status enum('san', 'usb', 'missing'));
alter table original add index idx_d (d);
alter table original add index idx_path (path);
alter table original add index idx_filename (filename);

-- 1 down
drop table original;
