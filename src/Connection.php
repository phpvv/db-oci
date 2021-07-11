<?php declare(strict_types=1);

/*
 * This file is part of the VV package.
 *
 * (c) Volodymyr Sarnytskyi <v00v4n@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace VV\Db\Oci;

use VV\Db\Driver\QueryInfo;

/**
 * Class Connection
 *
 * @package VV\Db\Oci
 */
class Connection implements \VV\Db\Driver\Connection
{

    private mixed $ociConn;

    /**
     * Connection constructor.
     *
     * @param mixed $ociConn
     */
    public function __construct(mixed $ociConn)
    {
        $this->ociConn = $ociConn;
    }

    /**
     * @inheritdoc
     */
    public function prepare(QueryInfo $query): \VV\Db\Driver\Statement
    {
        $stmt = Driver::ociParse($this->ociConn, $query->getString());

        return new Statement($stmt, $this->ociConn);
    }

    /**
     * @inheritDoc
     */
    public function startTransaction(): void
    {
    }

    /**
     * @inheritdoc
     */
    public function commit(bool $autocommit = false): void
    {
        if ($this->ociConn) {
            oci_commit($this->ociConn);
        }
    }

    /**
     * @inheritdoc
     */
    public function rollback(): void
    {
        if ($this->ociConn) {
            oci_rollback($this->ociConn);
        }
    }

    /**
     * @inheritdoc
     */
    public function disconnect(): void
    {
        oci_close($this->ociConn);
        $this->ociConn = null;
    }
}
