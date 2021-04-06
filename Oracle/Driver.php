<?php declare(strict_types=1);

/*
 * This file is part of the VV package.
 *
 * (c) Volodymyr Sarnytskyi <v00v4n@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace VV\Db\Oracle;

use VV\Db\Driver\Driver as DriverAlias;
use VV\Db\Driver\QueryStringifiers;
use VV\Db\Exceptions\ConnectionError as ConnError;
use VV\Db\Sql;

/**
 * Class Oracle
 *
 * @package VV\Db\Driver
 */
class Driver implements \VV\Db\Driver\Driver {

    private array $sessionParams = [
        'nls_numeric_characters' => '\'. \'',
        'nls_sort' => 'binary_ci',
        'nls_comp' => 'linguistic',
    ];

    public function connect(string $host, string $user, string $passwd, ?string $scheme, ?string $charset): Connection {

        $conn = @oci_new_connect($u = $user, $passwd, $host, $charset ?: '');
        if (!$conn) throw new ConnError(null, null, $this->ociError());

        $sessionParams = $this->sessionParams + [
                'nls_date_format' => '\'YYYY-MM-DD HH24:MI:SS\'',
                'nls_timestamp_format' => '\'YYYY-MM-DD HH24:MI:SSXFF\'',
                'nls_timestamp_tz_format' => '\'YYYY-MM-DD HH24:MI:SSXFF TZR\'',
            ];

        if (strtolower($u) != strtolower($sch = $scheme)) {
            $sessionParams['current_schema'] = $sch;
        }

        foreach ($sessionParams as $p => $v) {
            $stmt = self::ociParse($conn, "ALTER SESSION SET $p = $v");
            self::ociExecute($stmt);
        }
        $res = oci_commit($conn);
        if (!$res) throw new ConnError('Can\'t commit session settings');

        return new Connection($conn);
    }

    /**
     * @param Sql\SelectQuery $query
     *
     * @return QueryStringifiers\SelectStringifier
     */
    public function createSelectStringifier(Sql\SelectQuery $query): QueryStringifiers\SelectStringifier {
        return new SqlStringifier\SelectStringifier($query, $this);
    }

    /**
     * @param Sql\InsertQuery $query
     *
     * @return QueryStringifiers\InsertStringifier
     */
    public function createInsertStringifier(Sql\InsertQuery $query): QueryStringifiers\InsertStringifier {
        return new SqlStringifier\InsertStringifier($query, $this);
    }

    /**
     * @param Sql\UpdateQuery $query
     *
     * @return QueryStringifiers\UpdateStringifier
     */
    public function createUpdateStringifier(Sql\UpdateQuery $query): QueryStringifiers\UpdateStringifier {
        return new SqlStringifier\UpdateStringifier($query, $this);
    }

    /**
     * @param Sql\DeleteQuery $query
     *
     * @return QueryStringifiers\DeleteStringifier
     */
    public function createDeleteStringifier(Sql\DeleteQuery $query): QueryStringifiers\DeleteStringifier {
        return new SqlStringifier\DeleteStringifier($query, $this);
    }

    /**
     * @param array $sessionParams
     *
     * @return $this
     */
    public function setSessionParams(array $sessionParams): self {
        $this->sessionParams = $sessionParams;

        return $this;
    }

    /**
     * @return string
     */
    public function dbmsName(): string {
        return DriverAlias::DBMS_ORACLE;
    }

    public static function ociParse($connection, $sqlText) {
        $h = @oci_parse($connection, $sqlText);
        if (!$h) {
            throw new \VV\Db\Exceptions\SqlSyntaxError(null, null, self::ociError($connection));
        }

        return $h;
    }

    public static function ociExecute($stmt) {
        $h = @oci_execute($stmt, OCI_NO_AUTO_COMMIT);
        if (!$h) {
            throw new \VV\Db\Exceptions\SqlExecutionError(null, null, self::ociError($stmt));
        }
    }

    /**
     * @param resource $handle
     *
     * @return OciError|null
     */
    public static function ociError($handle = null) {
        $e = $handle ? oci_error($handle) : oci_error();
        if (!$e) return null;

        $message = $e['message'];
        $code = $e['code'];
        if ($sql = $e['sqltext']) {
            $o = $e['offset'];
            $sql = substr($sql, 0, $o)
                   . '<b style="color: red; font-size: 2em;">' . substr($sql, $o, 1) . '</b>'
                   . substr($sql, $o + 1);
            $message .= "\nOffset: $o\n$sql\n";
        }

        return new OciError($message, $code);
    }
}
