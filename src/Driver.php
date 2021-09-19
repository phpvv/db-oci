<?php

/*
 * This file is part of the VV package.
 *
 * (c) Volodymyr Sarnytskyi <v00v4n@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace VV\Db\Oci;

use VV\Db\Exceptions\ConnectionError;
use VV\Db\Exceptions\SqlExecutionError;
use VV\Db\Exceptions\SqlSyntaxError;
use VV\Db\Sql\Stringifiers\Factory as StringifiersFactory;

/**
 * Class Driver
 *
 * @package VV\Db\Oci
 */
class Driver implements \VV\Db\Driver\Driver
{

    private array $sessionParams = [
        'nls_numeric_characters' => '\'. \'',
        'nls_sort' => 'binary_ci',
        'nls_comp' => 'linguistic',
    ];

    public function connect(string $host, string $user, string $passwd, ?string $scheme, ?string $charset): Connection
    {
        $conn = @oci_new_connect($u = $user, $passwd, $host, $charset ?: '');
        if (!$conn) {
            throw new ConnectionError(null, null, $this->ociError());
        }

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
        if (!$res) {
            throw new ConnectionError('Can\'t commit session settings');
        }

        return new Connection($conn);
    }

    /**
     * @return string
     */
    public function getDbmsName(): string
    {
        return self::DBMS_ORACLE;
    }

    public function getSqlStringifiersFactory(): ?StringifiersFactory
    {
        return null;
    }

    /**
     * @param array $sessionParams
     *
     * @return $this
     */
    public function setSessionParams(array $sessionParams): static
    {
        $this->sessionParams = $sessionParams;

        return $this;
    }

    public static function ociParse($connection, $sqlText)
    {
        $h = @oci_parse($connection, $sqlText);
        if (!$h) {
            throw new SqlSyntaxError(previous: self::ociError($connection));
        }

        return $h;
    }

    public static function ociExecute($stmt)
    {
        $h = @oci_execute($stmt, OCI_NO_AUTO_COMMIT);
        if (!$h) {
            throw new SqlExecutionError(previous: self::ociError($stmt));
        }
    }

    /**
     * @param resource $handle
     *
     * @return OciError|null
     */
    public static function ociError($handle = null): ?OciError
    {
        $e = $handle ? oci_error($handle) : oci_error();
        if (!$e) {
            return null;
        }

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
