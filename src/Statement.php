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

use VV\Db\Param;

/**
 * Class Statement
 *
 * @package VV\Db\Oci
 */
class Statement implements \VV\Db\Driver\Statement
{

    private mixed $stmt;
    private mixed $ociConn;
    private ?array $lobsToUpload = [];
    private ?Param $insertedIdParam = null;

    /**
     * Prepared constructor.
     *
     * @param mixed $stmt
     * @param mixed $ociConn
     */
    public function __construct(mixed $stmt, mixed $ociConn)
    {
        $this->stmt = $stmt;
        $this->ociConn = $ociConn;
    }

    /**
     * @inheritdoc
     */
    public function bind(array $params): void
    {
        /** @var Param[] $lobs */
        $lobs = []; // bind LOB params at the end
        if ($params) {
            $i = 0;
            foreach ($params as $k => &$param) {
                if ($param instanceof Param && $param->isForInsertedId()) {
                    $this->insertedIdParam = $param;
                }

                if (is_string($k)) {
                    $name = ":$k";
                } elseif ($param instanceof Param && ($n = $param->getName())) {
                    $name = ":$n";
                } else {
                    $name = ':p' . (++$i);
                }

                if ($param instanceof Param) {
                    switch ($param->getType()) {
                        // LOBs to end
                        case Param::T_TEXT:
                        case Param::T_BLOB:
                            $lobs[$name] = $param;
                            break;

                        default:
                            $this->ociBindParam(
                                $name,
                                $param->getValue(),
                                $this->toOciType($param),
                                $param->getSize() ?: -1
                            );
                    }
                    $param->setBound();
                } else {
                    $this->ociBindParam($name, $param, $this->toOciType($param));
                }
            }
            unset($param);
        }

        // bind deferred LOB params
        $this->lobsToUpload = [];
        foreach ($lobs as $name => $param) {
            $lobDescriptor = oci_new_descriptor($this->ociConn, OCI_D_LOB);
            if ($param->isForUpload()) {
                $this->lobsToUpload[] = [$lobDescriptor, $param];
            }

            $this->ociBindParam($name, $lobDescriptor, $this->toOciType($param));
        }
    }

    /**
     * @inheritdoc
     */
    public function exec(): \VV\Db\Driver\Result
    {
        // execute query
        Driver::ociExecute($this->stmt);

        // upload LOBs
        /** @var \OCI_Lob $lobDescriptor */
        /** @var Param $param */
        foreach ($this->lobsToUpload as [$lobDescriptor, $param]) {
            if (!$value = $param->getValue()) {
                continue;
            }

            $lobDescriptor->rewind();
            foreach ($value as $block) {
                $lobDescriptor->write($block);
            }
        }

        $insertedId = $this->insertedIdParam?->getValue();

        return new Result($this->stmt, $insertedId);
    }

    /**
     * @inheritdoc
     */
    public function setFetchSize(int $size): void
    {
        oci_set_prefetch($this->stmt, $size);
    }

    /**
     * @inheritdoc
     */
    public function close(): void
    {
        oci_free_statement($this->stmt);
        $this->stmt = null;

        foreach ($this->lobsToUpload as [$lobDescriptor]) {
            /** @var \OCI_Lob $lobDescriptor */
            $lobDescriptor->close();
        }
        $this->lobsToUpload = [];
    }

    /**
     * @param     $name
     * @param     $value
     * @param     $type
     * @param int $size
     */
    private function ociBindParam($name, &$value, $type, int $size = -1)
    {
        $ob = oci_bind_by_name($this->stmt, $name, $value, $size, $type);

        if (!$ob) {
            $strValue = is_scalar($value) ? (string)$value : '(' . gettype($value) . ')';

            throw new \RuntimeException(
                'Bind params error: ' . $name . ' ' . $strValue,
                null,
                Driver::ociError($this->stmt)
            );
        }
    }

    /**
     * @param mixed $value
     *
     * @return int
     */
    private function toOciType(mixed $value): int
    {
        $paramType = null;
        if ($value instanceof Param) {
            $paramType = $value->getType();
            $value = $value->getValue();
        }

        if ($paramType) {
            return match ($paramType) {
                Param::T_INT => SQLT_INT,
                Param::T_BOOL => SQLT_BOL,
                Param::T_FLOAT,
                Param::T_STR => SQLT_CHR,
                Param::T_TEXT => SQLT_CLOB,
                Param::T_BLOB => SQLT_BLOB,
                Param::T_BIN => SQLT_BIN,
                default => throw new \LogicException('Not supported yet'),
            };
        }

        if (is_bool($value)) {
            return SQLT_BOL;
        }

        if (is_int($value)) {
            return SQLT_INT;
        }

        return SQLT_CHR;
    }
}
