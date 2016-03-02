use 5.010;
use Mojo::mysql;
use File::Basename;

my $batch       = 25_000;
my $c_report    = 100_000;
my $files_abort = 1_000_000;
my $images      = qr(\.tif$);

my $mysql = Mojo::mysql->new($ENV{MYSQL}=~/mysql/ ? $ENV{MYSQL} : "mysql://$ENV{MYSQL}:$ENV{MYSQL}@/$ENV{MYSQL}");
my $db = $mysql->db;

my $files = 0;
my $c = 0;
if ( my $table = $ARGV[0] ) {
  $table =~ s/\.\w+$//;
  $mysql->migrations->name($table)->from_data->migrate;
  $db->query("delete from $table");
  $db->query("alter table $table auto_increment = 1");
  @_ = ();
  while ( <> ) {
    printf "%08d | %08d\n", $c, $files if $c % $c_report == 0;
    $c++;
    s/\s*$//;
    next if /\\$/;
    my ($date) = (/\\(\d{8})\\/);
    my ($path, $file) = (/(.+\\)(.+)/);
    next unless $date && $file =~ $images;
    push @_, $date, ($path||''), ($file||'');
    next unless $#_ == $batch*3-1;
    my $values = join ',', map { '(?,?,?)' } 0..$#_/3;
    $files = $db->query("insert into $table (d, path, filename) values $values", @_)->last_insert_id;
    @_ = ();
    last if $ENV{ABORT} && $files >= $files_abort;
  }
  if ( @_ ) {
    my $values = join ',', map { '(?,?,?)' } 0..$#_/3;
    $files = $db->query("insert into $table (d, path, filename) values $values", @_)->last_insert_id;
  }
} else {
  # select 'original' t,count(*) c from original union select 'backups' t,count(*) c  from backups union select 'recovery' t,count(*) c  from recovery;
  # select count(*) 'missing from backups' from original where filename not in (select filename from backups);  
  # select count(*) 'missing from recovery' from original where filename not in (select filename from recovery);
  # select count(*) 'missing from backups but available in recovery' from recovery where filename in 
  #   (select filename from original where filename not in (select filename from backups)) order by d;
  # missing_from_backups_but_available_in_recovery
  my $recovery = $db->query('select * from recovery where filename in (select filename from original where filename not in (select filename from backups)) order by d');
  while ( my $file = $recovery->hash ) {
    printf "mkdir '%s'\n", $file->{path};
    printf "copy '%s' '%s'\n", $file->{filename}, $file->{path};
  }
}

__DATA__
@@ sample.data
shuf 1395162.txt | head -100000 > sample.txt
export R=100 MYSQL=dbname; shuf sample.txt | head -$R > original.txt ; shuf original.txt | head -$(echo "scale=0;$R*.9/1"|bc) > backups.txt ; shuf original.txt | head -$(echo "scale=0;$R*.8/1"|bc) > recovery.txt ; wc -l [a-z]*.txt
for i in original.txt backups.txt recovery.txt; do date ; time perl recover_missing_files.pl $i ; date; done

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
