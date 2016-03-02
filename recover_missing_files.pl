use 5.010;
use Mojo::mysql;
use File::Basename;
use Getopt::Long qw(GetOptionsFromArray :config no_auto_abbrev no_ignore_case);
use Time::HiRes qw(gettimeofday tv_interval);

my $images = qr(\.tif$);

GetOptions(
  'S=i' => \(my $S = 100_000),  # Grab a sampling of the full data set
  's=i' => \my $s,              # Create a sampling of that sampling
  'a=i' => \my $files_abort,    # Max records to process
);

die "MYSQL not defined\n" unless $ENV{MYSQL};
my $mysql = Mojo::mysql->new($ENV{MYSQL}=~/mysql/ ? $ENV{MYSQL} : "mysql://$ENV{MYSQL}:$ENV{MYSQL}@/$ENV{MYSQL}");
my $db = $mysql->db;

if ( my $file = $ARGV[0] ) {
  if ( $s ) {
    if ( -e $file ) {
      sample($file, $S, 'sample.txt');
      sample('sample.txt', $s, 'original.txt');
      sample('original.txt', $s*.9, 'backups.txt');
      sample('original.txt', $s*.8, 'recovery.txt');
      print qx(wc -l [a-z]*.txt);
      foreach my $file (qw(original.txt backups.txt recovery.txt)) { load($file) }
    }
  } elsif ( $file =~ /(original|backups|recovery)/ ) {
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
  my $file = shift;
  my $start = [gettimeofday];
  my $table = $file;
  $table =~ s/\.\w+$//;
  my $files = 0;
  my $c = 0;
  $mysql->migrations->name($table)->from_data->migrate;
  $db->query("delete from $table");
  $db->query("alter table $table auto_increment = 1");
  local @_ = ();
  open IN, $file;
  1 while <IN>;
  my $batch = int($.*.1);
  seek(IN,0,0);
  print "$file\n";
  while ( local $_ = <IN> ) {
    printf "  %08d | %08d (%s)\r", $c, $files, tv_interval($start) if $c % int($batch*.1) == 0;
    $c++;
    s/\s*$//;
    next if /\\$/;
    my ($date) = (/\\(\d{8})\\/);
    my ($path, $file) = (/(.+\\)(.+)/);
    next unless $date && $file =~ $images;
    push @_, $date, ($path||''), ($file||'');
    next unless $#_ == $batch*3-1;
    my $values = join ',', map { '(?,?,?)' } 0..$#_/3;
    $files = $db->query("insert into $table (d, path, filename) values $values", @_)->last_insert_id - 1;
    @_ = ();
    last if $files_abort && $files >= $files_abort;
  }
  close IN;
  if ( @_ ) {
    printf "  %08d | %08d (%s)\r", $c, $files, tv_interval($start);
    my $values = join ',', map { '(?,?,?)' } 0..$#_/3;
    $files = $db->query("insert into $table (d, path, filename) values $values", @_)->last_insert_id;
  }
  print "\n";
}

__DATA__
@@ recovery
-- 1 up
create table recovery (id int primary key auto_increment, d date, path varchar(1024), filename varchar(64), mtime datetime, size int, status enum('san', 'usb', 'missing'));
alter table recovery add index idx_d (d);
alter table recovery add index idx_path (path);
alter table recovery add index idx_filename (filename);

-- 1 down
drop table recovery;

@@ backups
-- 1 up
create table backups (id int primary key auto_increment, d date, path varchar(1024), filename varchar(64), mtime datetime, size int, status enum('san', 'usb', 'missing'));
alter table backups add index idx_d (d);
alter table backups add index idx_path (path);
alter table backups add index idx_filename (filename);

-- 1 down
drop table backups;

@@ original
-- 1 up
create table original (id int primary key auto_increment, d date, path varchar(1024), filename varchar(64), mtime datetime, size int, status enum('san', 'usb', 'missing'));
alter table original add index idx_d (d);
alter table original add index idx_path (path);
alter table original add index idx_filename (filename);

-- 1 down
drop table original;
