<?php
/*
This is an example of PHP UserDB API that an Espion agent can use to store web scraping
result. The API use PostgreSQL as database, please modify the DB class if want to use
other databases.

Please check php.ini for necessary config.
*/

ini_set('display_errors', 1);
date_default_timezone_set('UTC'); // Please change the timezone value

// Please modify your db info accordingly
$db_host='localhost';
$db_name='the_userdb_name';
$db_user='the_db_user';
$db_pass='the_password';
$db_table = 'the_table_name';

try {
    $API = new UserDBAPI($_REQUEST);
    echo $API->process();
} catch (Exception $e) {
    echo json_encode(Array('status' => 'error', 'message' => $e->getMessage()));
}

class MethodNotAllowed extends Exception { }

class UserDBAPI
{
    protected $method = '';
    protected $action = '';
    protected $args = '';
    protected $data = null;

    public function __construct($request, $db = null)
    {
        header("Access-Control-Allow-Orgin: *");
        header("Access-Control-Allow-Methods: *");
        header("Content-Type: application/json");

        $this->method = $_SERVER['REQUEST_METHOD'];
        $this->action = $request['method'];
        unset($request['method']);
        $test = true;
        $this->args = $request;

        global $db_host, $db_user, $db_pass, $db_name, $db_table;
        $this->db = new DB($db_host, $db_user, $db_pass, $db_name, $db_table);

        if ($this->method == 'POST' && array_key_exists('HTTP_X_HTTP_METHOD', $_SERVER)) {
            if ($_SERVER['HTTP_X_HTTP_METHOD'] == 'DELETE') {
                $this->method = 'DELETE';
            } else if ($_SERVER['HTTP_X_HTTP_METHOD'] == 'PUT') {
                $this->method = 'PUT';
            } else {
                throw new Exception("Unexpected Header");
            }
        }

        switch($this->method) {
            case 'DELETE':
            case 'POST':
            $this->request = $this->_cleanInputs($_POST);
            $this->data = file_get_contents("php://input");
            break;
            case 'GET':
            $this->request = $this->_cleanInputs($_GET);
            break;
            case 'PUT':
            $this->request = $this->_cleanInputs($_GET);
            $this->file = file_get_contents("php://input");
            break;
            default:
            $this->_response('Invalid Method', 405);
            break;
        }
    }

    public function process()
    {
        if (method_exists($this, $this->action)) {
            try {
                try {
                    $resp = $this->{$this->action}($this->args);
                    return $this->_response($resp);
                } catch (MethodNotAllowed $e) {
                    return $this->_response(array('status'=>'error','message'=>$e->getMessage()), 405);
                }
            } catch(Exception $e) {
                return $this->_response(array('status'=>'error','message'=>$e->getMessage()), 505);
            }
        }
        return $this->_response(array('status'=>'error','message'=>'You should select a request method'), 404);
    }

    private function _response($data, $status = 200)
    {
        header("HTTP/1.1 " . $status . " " . $this->_requestStatus($status));
        return json_encode($data);
    }

    private function _cleanInputs($data)
    {
        $clean_input = Array();
        if (is_array($data)) {
            foreach ($data as $k => $v) {
                $clean_input[$k] = $this->_cleanInputs($v);
            }
        } else {
            $clean_input = trim(strip_tags($data));
        }
        return $clean_input;
    }

    private function _requestStatus($code)
    {
        $status = array(
            200 => 'OK',
            404 => 'Not Found',
            405 => 'Method Not Allowed',
            500 => 'Internal Server Error',
        );
        return ($status[$code])?$status[$code]:$status[500];
    }

    /* action: record_items */
    protected function record_items($args)
    {
        if ($this->method != 'POST') throw new MethodNotAllowed('Only POST is accepted');

        $DATA = json_decode($this->data);
        $itemFields = Array('id', 'type', '__dt__', '__ttl__', '__partial_update__');

        if(empty($DATA->items)) {
            return Array('status'=>'error','message'=>'You should provide at least one item');
        }

        foreach ($DATA->items AS $item) {
            $__dt__ = NULL;
            if(!isset($item->type)) $item->type = '';
            if(isset($item->__partial_update__)) $item->__partial_update__ = true;
            else                                 $item->__partial_update__ = false;
            if(empty($item->__dt__))    $__dt__ = time();
            else                        $__dt__ = strtotime($item->__dt__);
            if(!empty($item->__ttl__))  $__dt__ = date('Y-m-d h:i:s',$__dt__ - $item->__ttl__);
            else                        $__dt__ = date('Y-m-d h:i:s',$__dt__);
            $expires = strtotime($__dt__)+604800;
            $active = (strtotime($__dt__)+604800>time()) ? 1: 0;
            $attributes = array();
            $history = array();
            $seen_before = $this->db->retriv(array('first_seen_on','attributes','history'),"WHERE id='".pg_escape_string($item->id)."' AND type='".pg_escape_string($item->type)."'");

            if (!empty($seen_before)) {
                $history = json_decode($seen_before['history']);
                $attributes = json_decode($seen_before['attributes']);
            }

            foreach ($item AS $k=>$v) {
                if ($k[0]=='$') {
                    $key = substr($k,1);
                    if(!empty($history)) array_push($history->$key,array($__dt__,$v));
                    else                 $history[$key] = array(array($__dt__,$v));
                } elseif (!in_array($k,$itemFields)) {
                    if(isset($attributes->$k))  $attributes->$k = $v;
                    else                        $attributes[$k] = $v;
                }
            }
            $attributes = json_encode($attributes);

            if (!empty($history))    $history = json_encode($history);

            $data = array('id'=>$item->id, 'type'=>$item->type,'last_seen_on'=>$__dt__, 'active'=>$active,'expires_on'=>date('Y-m-d h:i:s',$expires), 'attributes'=>$attributes);

            if (empty($seen_before))     $data = array_merge($data,array('uuid'=>$this->db->guidv4(),'first_seen_on'=>$__dt__));
            if (!$item->__partial_update__)      $data = array_merge($data,array('fully_updated_on'=>$__dt__));
            if (!empty($history))        $data = array_merge($data,array('history'=>$history));

            try {
                $this->db->insert($data, array_keys($data), Array('id','type'));
            } catch(Exception $e) {
                return Array('status'=>'errorInsert', 'message'=>$e->getMessage());
            }
        }

        return Array('status'=>'ok');
    }

    /* action: item_exists */
    protected function item_exists()
    {
        if ($this->method != 'GET') throw new MethodNotAllowed('Only GET is accepted');

        if (empty($this->args['id']) || !isset($this->args['type'])) {
            return Array('status'=>'error','message'=>'You should provide both id and type');
        }

        $updated_after = empty($this->args['updated_after']) ? '' : " AND fully_updated_on>'".pg_escape_string($this->args['updated_after'])."'";
        return Array('status'=>'ok', 'exists'=>$this->db->verif("WHERE id='".pg_escape_string($this->args['id'])."' AND type='".pg_escape_string($this->args['type'])."'$updated_after") ? true: false);
    }

    /* action: items_exist */
    protected function items_exist()
    {
        if ($this->method != 'POST') throw new MethodNotAllowed('Only POST is accepted');

        $DATA = json_decode($this->data);
        if (!isset($DATA->type) || empty($DATA->ids)) {
            return Array('status'=>'error','message'=>'You should provide at least one id');
        }

        $ids = Array();
        foreach ($DATA->ids AS $id) {
            $updated_after = empty($DATA->updated_after) ? '' : " AND fully_updated_on>'".pg_escape_string($DATA->updated_after)."'";
            $ids[$id] = $this->db->verif("WHERE id='".pg_escape_string($id)."' AND type='".pg_escape_string($DATA->type)."'$updated_after") ? true: false;
        }
        return Array('status'=>'ok', 'exist'=>$ids);
    }

    /* action - query */
    protected function query()
    {
        if ($this->method != 'GET') throw new MethodNotAllowed('Only GET is accepted');

        foreach ($this->args AS $q=>$v) {
            if ($q=='seen_before')      $conds[] = "first_seen_on < 'first_seen_on'";
            elseif ($q=='seen_after')   $conds[] = "first_seen_on > 'first_seen_on'";
            elseif ($q=='active')       $conds[] = "active IS TRUE";
            elseif ($q=='id' || $q=='type') {
                $iOr = array();
                if (is_array($v)) {
                    foreach ($v AS $vv) {
                        $iOr[] = "$q='$vv'";
                    }
                    $conds[] = "(".implode(' OR ',$iOr).")";
                } else {
                    $conds[] = "$q='$v'";
                }
            } else {
                if (is_array($v)) {
                    foreach ($v AS $vv) {
                        $Or[] = "attributes->>'$q'='$vv'";
                    }
                    $conds[] = "(".implode(' OR ',$Or).")";
                } else {
                    $conds[] = "attributes->>'$q'='$v'";
                }
            }
        }

        if(!empty($conds)) $conds = implode(' AND ', $conds);
        else               $conds = '1=1';
        $items = $this->db->retriv_array(array('id','type','first_seen_on','last_seen_on','fully_updated_on','active','attributes','history'),"WHERE $conds",0,0,0);

        if (empty($items)) {
            return Array('status'=>'ok', 'items'=>null);
        }

        foreach ($items AS &$item) {
            $item['attributes'] = json_decode($item['attributes']);
            $item['history'] = json_decode($item['history']);
        }
        return Array('status'=>'ok', 'items'=>$items);
    }
}

class DBException extends Exception {}

class DB
{
    protected $_pdc = null;
    protected $table = '';
    protected $test = false;

    public function __construct($host, $user, $pass, $db, $table, $test = false)
    {
        if (empty($host)) $host = 'localhost';
        if (empty($db) || empty($user)) {
            throw new Exception("Database error: Please make sure you specified the database name & user.");
        }
        try {
            $this->_pdc = new PDO("pgsql:host=$host;port=5432;dbname=$db;",$user,$pass);
        } catch(Exception $e) {
            throw new Exception($e->getMessage());
        }
        $this->test = $test;
        if ($test === true) $this->table = $table . "_test";
        else                $this->table = $table;
        $this->create_userdb_table($this->table);
    }

    public function __destruct()
    {
        $this->_pdc = null;
    }

    public function retriv($tableau, $condition)
    {
        $table = $this->table;
        $result = $this->_pdc->query("SELECT \"".implode('", "',$tableau)."\" FROM \"$table\" $condition");
        $err=$this->_pdc->errorInfo();
        if ($err[2]) throw new Exception('DB Error: '.$err[2]);
        return $result->fetch(PDO::FETCH_ASSOC);
    }

    public function retriv_array($tableau, $condition)
    {
        $table = $this->table;
        $sql    =   "SELECT ".'"'.implode('", "',$tableau).'"'." FROM \"$table\" $condition";
        $result =   $this->_pdc->query($sql);
        $err = $this->_pdc->errorInfo();
        if ($err[2]) throw new Exception('DB Error: '.$err[2]);
        $double_row = $result->fetchAll(PDO::FETCH_ASSOC);
        if (!empty($double_row)) return $double_row;
        else                     return false;
    }

    public function verif($condition)
    {
        $table = $this->table;
        $sql    =   "SELECT count(*) AS n FROM \"$table\" $condition";
        $result =   $this->_pdc->query($sql);
        $err = $this->_pdc->errorInfo();
        if($err[2]) throw new Exception('DB Error: '.$err[2]);
        $r  =   $result->fetch(PDO::FETCH_ASSOC);
        return $r['n'];
    }

    public function insert($Data, $Fields, $UNIQUE=false, $returning=false)
    {
        $table = $this->table;
        foreach ($Fields AS $k=>$Field) {
            if(!isset($Data[$Field])) unset($Fields[$k]);
        }
        $column = '"'.implode('" , "',$Fields).'"';
        $value = implode(" , :",$Fields);
        foreach ($Fields AS $Field) {
            $cData[$Field] = $Data[$Field];
            $dKey[] = "\"$Field\"= :$Field";
        }

        $sql = "INSERT INTO \"$table\" ($column) VALUES (:$value)";
        $cond = "WHERE 1=1 ";
        foreach ($UNIQUE AS $u)  $cond .=" AND \"$u\"='".pg_escape_string($Data[$u])."'";
        if ($this->verif($cond)) $sql  = "UPDATE \"$table\" SET ".implode(',',$dKey)." ".$cond;
        $query = $this->_pdc->prepare($sql);
        if (!$query) {
            $err=$this->_pdc->errorInfo();
            throw new Exception('Error: '.$err[2].'<br />'.$sql);
        }
        if (!$query->execute($cData)){
            $err=$query->errorInfo();
            $value="'".implode("' , '",$cData)."'";
            throw new Exception('Error: '.$err[2].'<br />'.$sql);
        }
        return $query->fetchAll(PDO::FETCH_ASSOC);
    }

    public function guidv4()
    {
        $data = openssl_random_pseudo_bytes(16);
        assert(strlen($data) == 16);
        $data[6] = chr(ord($data[6]) & 0x0f | 0x40); // set version to 0100
        $data[8] = chr(ord($data[8]) & 0x3f | 0x80); // set bits 6-7 to 10
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }

    protected function create_userdb_table($table)
    {
        try {
            $sql_create_userdb = <<<EOSQL
CREATE TABLE IF NOT EXISTS $table (
uuid UUID primary key not null,
"type" varchar not null,
id varchar not null,
attributes jsonb,
history jsonb,
first_seen_on timestamp without time zone,
last_seen_on timestamp without time zone,
fully_updated_on timestamp without time zone,
active boolean default true,
expires_on timestamp without time zone,
unique ("type", id)
)
EOSQL;
            $this->_pdc->exec($sql_create_userdb);
        } catch(Exception $e) {
            throw new Exception($e->getMessage());
        }
    }
}
?>
