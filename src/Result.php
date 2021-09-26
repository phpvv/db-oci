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

use VV\Db;
use VV\Db\Driver\Result as ResultInterface;

/**
 * Class Result
 *
 * @package VV\Db\Oci
 */
class Result implements ResultInterface
{
    private mixed $stmt;
    private int|string|null $insertedId;

    public function __construct(mixed $stmt, int|string|null $insertedId)
    {
        $this->stmt = $stmt;
        $this->insertedId = $insertedId;
    }

    public function getIterator(int $flags): \Traversable
    {
        $ociFlags = OCI_RETURN_NULLS;

        if ($fetchAssoc = (bool)($flags & Db::FETCH_ASSOC)) {
            $ociFlags |= OCI_ASSOC;
        }
        if ($flags & Db::FETCH_NUM) {
            $ociFlags |= OCI_NUM;
        }
        if (!($flags & Db::FETCH_LOB_OBJECT)) {
            $ociFlags |= OCI_RETURN_LOBS;
        }

        while ($row = oci_fetch_array($this->stmt, $ociFlags)) {
            if ($fetchAssoc) {
                $row = array_change_key_case($row, CASE_LOWER);
            }
            yield $row;
        }
    }

    public function getInsertedId(): int|string|null
    {
        return $this->insertedId;
    }

    public function getAffectedRows(): int
    {
        return oci_num_rows($this->stmt);
    }

    /**
     * Closes statement
     */
    public function close(): void
    {
    }
}
